# app/api/routes_docs.py
from __future__ import annotations

import math
import re
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text
from app.db import engine

router = APIRouter()  # main aplica prefixo /api


# -------------------------
# Helpers
# -------------------------

def _wb_regex(phrase: str) -> re.Pattern:
    """
    Constrói um regex 'word-boundary' simples para a frase (tolerante a espaços múltiplos).
    Ex.: "reasonable accommodation" → r'\breasonable\s+accommodation\b' (case-insensitive)
    """
    # escapa e troca espaços por \s+
    p = re.escape(phrase.strip())
    p = re.sub(r"\s+", r"\\s+", p)
    return re.compile(rf"\b{p}\b", re.IGNORECASE | re.UNICODE)


def _fetch_doc(doc_id: int) -> Dict:
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT id, doc_name, file_path, sha256, lang, status,
                   sentence_count, evidence_count, created_at, updated_at
              FROM documents
             WHERE id = :doc_id
        """), {"doc_id": doc_id}).mappings().first()
    if not row:
        raise HTTPException(404, f"document id={doc_id} not found")
    return dict(row)


def _fetch_concept_count() -> int:
    with engine.begin() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM concepts")).scalar()
    return int(n or 0)


# -------------------------
# GET /api/docs (lista)
# -------------------------
@router.get("/docs")
def list_docs(
    q: Optional[str] = Query(None, description="substring em doc_name"),
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    where = ["1=1"]
    params: Dict = {"limit": limit, "offset": offset}
    if q:
        where.append("doc_name ILIKE :q")
        params["q"] = f"%{q}%"
    if status:
        where.append("status = :status")
        params["status"] = status

    sql = f"""
        SELECT id, doc_name, status, lang,
               sentence_count, evidence_count,
               created_at, updated_at
          FROM documents
         WHERE {' AND '.join(where)}
         ORDER BY updated_at DESC NULLS LAST, id DESC
         LIMIT :limit OFFSET :offset
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    rows = [dict(r) for r in rows]                     # <-- garante dicts reais
    return rows  # <-- simples


# -------------------------
# GET /api/docs/{doc_id}
# -------------------------
@router.get("/docs/{doc_id}")
def get_doc(doc_id: int):
    return _fetch_doc(doc_id)  # <-- simples


# -------------------------
# RECOMPUTE (núcleo)
# -------------------------

def _recompute_for_doc(doc_id: int) -> Dict:
    """
    Recalcula:
      - doc_concept_scores (best_level, evidence_cnt)
      - doc_vg_mentions (vg_id, mention_cnt)
      - doc_cc_vg (co-ocorrência CC×VG por snippet)
      - doc_equiframe_indices (cc_covered, cc_quality_3p, vg_covered, %)
    usando apenas os SNIPPETS de evidences (piloto).
    """
    t0 = time.time()
    doc = _fetch_doc(doc_id)
    doc_name = doc["doc_name"]

    # 1) Puxa evidences do documento
    with engine.begin() as conn:
        ev_rows = conn.execute(text("""
            SELECT concept_id, COALESCE(level, 1) AS level, snippet, lang
              FROM evidences
             WHERE doc_name = :doc_name
        """), {"doc_name": doc_name}).mappings().all()

    # early exit
    if not ev_rows:
        # limpa agregados e zera indices
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM doc_concept_scores WHERE doc_id = :d"), {"d": doc_id})
            conn.execute(text("DELETE FROM doc_vg_mentions WHERE doc_id = :d"), {"d": doc_id})
            conn.execute(text("DELETE FROM doc_cc_vg WHERE doc_id = :d"), {"d": doc_id})
            conn.execute(text("""
                INSERT INTO doc_equiframe_indices (doc_id, cc_covered, cc_quality_3p, vg_covered,
                                                  pct_cc_covered, pct_cc_quality_3p, computed_at)
                VALUES (:d, 0, 0, 0, 0.0, 0.0, now())
                ON CONFLICT (doc_id) DO UPDATE
                   SET cc_covered = 0, cc_quality_3p = 0, vg_covered = 0,
                       pct_cc_covered = 0.0, pct_cc_quality_3p = 0.0, computed_at = now()
            """), {"d": doc_id})
            # atualiza contador de evidências no documents
            conn.execute(text("""
                UPDATE documents SET evidence_count = 0, updated_at = now() WHERE id = :d
            """), {"d": doc_id})
        return {
            "doc_id": doc_id, "evidence_rows": 0,
            "concept_scores": 0, "vg_mentions": 0, "matrix_rows": 0,
            "cc_covered": 0, "cc_quality_3p": 0, "vg_covered": 0,
            "elapsed_s": round(time.time() - t0, 3)
        }

    # 2) Agregados por conceito (best_level, count)
    best_level: Dict[int, int] = defaultdict(int)
    ev_count: Dict[int, int] = defaultdict(int)

    # também manter lista de snippets por conceito para a co-ocorrência com VG
    concept_snippets: Dict[int, List[str]] = defaultdict(list)
    lang_seen: Optional[str] = None

    for r in ev_rows:
        cid = int(r["concept_id"])
        lvl = int(r["level"] or 1)
        snip = (r["snippet"] or "").strip()
        best_level[cid] = max(best_level[cid], lvl)
        ev_count[cid] += 1
        if snip:
            concept_snippets[cid].append(snip)
        if not lang_seen and r["lang"]:
            lang_seen = r["lang"]

    # 3) Aplica overrides (se houver)
    overrides: Dict[int, int] = {}
    with engine.begin() as conn:
        for row in conn.execute(text("""
            SELECT concept_id, level FROM doc_concept_overrides
             WHERE doc_id = :doc_id
        """), {"doc_id": doc_id}).mappings():
            overrides[int(row["concept_id"])] = int(row["level"])

    final_level: Dict[int, int] = {}
    for cid, lvl in best_level.items():
        if cid in overrides:
            final_level[cid] = max(lvl, overrides[cid])
        else:
            final_level[cid] = lvl

    # 4) Upsert doc_concept_scores
    concept_rows = [{"doc_id": doc_id, "concept_id": cid,
                     "best_level": int(best_level[cid]),
                     "evidence_cnt": int(ev_count[cid])}
                    for cid in sorted(best_level.keys())]

    with engine.begin() as conn:
        # limpa antigo e insere novo (mais simples e rápido)
        conn.execute(text("DELETE FROM doc_concept_scores WHERE doc_id = :d"), {"d": doc_id})
        if concept_rows:
            conn.execute(text("""
                INSERT INTO doc_concept_scores (doc_id, concept_id, best_level, evidence_cnt)
                VALUES (:doc_id, :concept_id, :best_level, :evidence_cnt)
            """), concept_rows)

    # 5) Monta dicionário de VG (termos) pela língua do doc (fallback: qualquer)
    #    Para o piloto, usamos apenas termos (regex) e casamos nos SNIPPETS.    
    vg_terms: Dict[int, List[re.Pattern]] = defaultdict(list)
    with engine.begin() as conn:
        vgq = []  # vamos tentar por lang; se não achar, caímos no fallback (tudo)
        if lang_seen:
            ls = str(lang_seen).lower()
            vgq = conn.execute(text("""
                SELECT v.id AS vg_id, t.term
                  FROM vulnerable_groups v
                  JOIN vg_lexicon_terms t ON t.vg_id = v.id
                 WHERE LOWER(t.lang) = :lang
            """), {"lang": ls}).mappings().all()

        if not vgq:  # <-- fallback caso não haja termos na língua do doc
            vgq = conn.execute(text("""
                SELECT v.id AS vg_id, t.term
                  FROM vulnerable_groups v
                  JOIN vg_lexicon_terms t ON t.vg_id = v.id
            """)).mappings().all()

    for row in vgq:
        term = (row["term"] or "").strip()
        if not term:
            continue
        vg_terms[int(row["vg_id"])].append(_wb_regex(term))

    # 6) Conta menções de VG por documento + co-ocorrência CC×VG por SNIPPET
    vg_mention_cnt: Dict[int, int] = defaultdict(int)           # vg_id → total mentions
    cc_vg_cnt: Dict[Tuple[int, int], int] = defaultdict(int)    # (concept_id, vg_id) → co-occur

    if vg_terms:
        # pré-compacta lista de todos snippets do doc
        all_snippets: List[Tuple[int, str]] = []
        for cid, snips in concept_snippets.items():
            for s in snips:
                if s:
                    all_snippets.append((cid, s))

        # percorre cada snippet uma vez: detecta VGs presentes e agrega
        for cid, snip in all_snippets:
            present_vgs = set()
            for vg_id, patterns in vg_terms.items():
                # conta matches (soma das ocorrências de cada termo do vg)
                m = 0
                for rx in patterns:
                    m += len(rx.findall(snip))
                if m > 0:
                    vg_mention_cnt[vg_id] += m
                    present_vgs.add(vg_id)

            for vg_id in present_vgs:
                cc_vg_cnt[(cid, vg_id)] += 1  # 1 por snippet/CC/VG
    # else: nenhuma tabela de VG carregada → tudo zero

    # 7) Upsert doc_vg_mentions e doc_cc_vg
    vg_rows = [{"doc_id": doc_id, "vg_id": vg, "mention_cnt": int(n)}
               for vg, n in sorted(vg_mention_cnt.items())]

    matrix_rows = [{"doc_id": doc_id, "concept_id": cid, "vg_id": vg, "mention_cnt": int(n)}
                   for (cid, vg), n in sorted(cc_vg_cnt.items())]

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM doc_vg_mentions WHERE doc_id = :d"), {"d": doc_id})
        conn.execute(text("DELETE FROM doc_cc_vg WHERE doc_id = :d"), {"d": doc_id})
        if vg_rows:
            conn.execute(text("""
                INSERT INTO doc_vg_mentions (doc_id, vg_id, mention_cnt)
                VALUES (:doc_id, :vg_id, :mention_cnt)
            """), vg_rows)
        if matrix_rows:
            conn.execute(text("""
                INSERT INTO doc_cc_vg (doc_id, concept_id, vg_id, mention_cnt)
                VALUES (:doc_id, :concept_id, :vg_id, :mention_cnt)
            """), matrix_rows)

    # 8) Calcula índices EquiFrame e upsert em doc_equiframe_indices
    total_cc = max(1, _fetch_concept_count())  # evita div/0
    cc_covered = len([1 for cid, lvl in best_level.items() if lvl >= 1])
    cc_quality_3p = len([1 for cid, lvl in final_level.items() if lvl >= 3])
    vg_covered = len([1 for vg, n in vg_mention_cnt.items() if n > 0])

    pct_cc_covered = cc_covered / float(total_cc)
    pct_cc_quality_3p = cc_quality_3p / float(total_cc)

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO doc_equiframe_indices
                (doc_id, cc_covered, cc_quality_3p, vg_covered,
                 pct_cc_covered, pct_cc_quality_3p, computed_at)
            VALUES (:doc_id, :cc_cov, :cc_q3, :vg_cov, :p_cov, :p_q3, now())
            ON CONFLICT (doc_id) DO UPDATE
               SET cc_covered = EXCLUDED.cc_covered,
                   cc_quality_3p = EXCLUDED.cc_quality_3p,
                   vg_covered = EXCLUDED.vg_covered,
                   pct_cc_covered = EXCLUDED.pct_cc_covered,
                   pct_cc_quality_3p = EXCLUDED.pct_cc_quality_3p,
                   computed_at = now()
        """), {
            "doc_id": doc_id,
            "cc_cov": cc_covered,
            "cc_q3": cc_quality_3p,
            "vg_cov": vg_covered,
            "p_cov": pct_cc_covered,
            "p_q3": pct_cc_quality_3p,
        })

        # Atualiza contagem de evidências no documento (melhor esforço)
        total_evidences = sum(ev_count.values())
        conn.execute(text("""
            UPDATE documents
               SET evidence_count = :n, updated_at = now()
             WHERE id = :d
        """), {"n": int(total_evidences), "d": doc_id})

    return {
        "doc_id": doc_id,
        "evidence_rows": len(ev_rows),
        "concept_scores": len(concept_rows),
        "vg_mentions": len(vg_rows),
        "matrix_rows": len(matrix_rows),
        "cc_covered": cc_covered,
        "cc_quality_3p": cc_quality_3p,
        "vg_covered": vg_covered,
        "pct_cc_covered": pct_cc_covered,
        "pct_cc_quality_3p": pct_cc_quality_3p,
        "elapsed_s": round(time.time() - t0, 3)
    }


# -------------------------
# POST /api/docs/{doc_id}/recompute
# -------------------------
@router.post("/docs/{doc_id}/recompute")
def recompute_doc_indices(doc_id: int):
    res = _recompute_for_doc(doc_id)
    return _recompute_for_doc(doc_id)



# -------------------------
# GET /api/docs/{doc_id}/indices
# -------------------------
@router.get("/docs/{doc_id}/indices")
def get_doc_indices(doc_id: int):
    _ = _fetch_doc(doc_id)
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT doc_id, cc_covered, cc_quality_3p, vg_covered,
                   pct_cc_covered, pct_cc_quality_3p, computed_at
              FROM doc_equiframe_indices
             WHERE doc_id = :d
        """), {"d": doc_id}).mappings().first()
    if not row:
        return JSONResponse(content=jsonable_encoder({"doc_id": doc_id, "computed": False}))
    out = dict(row)
    out["computed"] = True
    return out  # <-- simples


# -------------------------
# GET /api/docs/{doc_id}/concept-scores
# -------------------------
@router.get("/docs/{doc_id}/concept-scores")
def get_doc_concept_scores(doc_id: int):
    _ = _fetch_doc(doc_id)
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT s.concept_id,
                   s.best_level,
                   s.evidence_cnt,
                   COALESCE(o.level, NULL) AS override_level,
                   GREATEST(s.best_level, COALESCE(o.level, 0)) AS final_level
              FROM doc_concept_scores s
              LEFT JOIN doc_concept_overrides o
                     ON o.doc_id = s.doc_id AND o.concept_id = s.concept_id
             WHERE s.doc_id = :d
             ORDER BY s.concept_id
        """), {"d": doc_id}).mappings().all()
    return [dict(r) for r in rows]  # <-- simples


# -------------------------
# GET /api/docs/{doc_id}/vg-mentions
# -------------------------
@router.get("/docs/{doc_id}/vg-mentions")
def get_doc_vg_mentions(doc_id: int):
    _ = _fetch_doc(doc_id)
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT m.vg_id, m.mention_cnt,
                   v.name_en, v.name_pt
              FROM doc_vg_mentions m
              JOIN vulnerable_groups v ON v.id = m.vg_id
             WHERE m.doc_id = :d
             ORDER BY m.mention_cnt DESC, m.vg_id
        """), {"d": doc_id}).mappings().all()
    return [dict(r) for r in rows]  # <-- simples



# -------------------------
# GET /api/docs/{doc_id}/matrix (CC×VG)
# -------------------------
@router.get("/docs/{doc_id}/matrix")
def get_doc_matrix(doc_id: int):
    _ = _fetch_doc(doc_id)
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT c.concept_id, c.vg_id, c.mention_cnt,
                   coalesce(vc.name_en, '') AS vg_name_en,
                   coalesce(vc.name_pt, '') AS vg_name_pt
              FROM doc_cc_vg c
              LEFT JOIN vulnerable_groups vc ON vc.id = c.vg_id
             WHERE c.doc_id = :d
             ORDER BY c.concept_id, c.vg_id
        """), {"d": doc_id}).mappings().all()
    return [dict(r) for r in rows]  # <-- simples
