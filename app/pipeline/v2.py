# app/pipeline/v2.py
import hashlib
from sqlalchemy import text
from app.db import engine

from .pdf import extract_pages_text
from .nlp import page_to_sentences
from .dict_repo import load_dictionary
from .matcher import match_sentence

def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8"), usedforsecurity=False).hexdigest()

def extract_pdf_to_sentences(doc_id: int) -> int:
    """Extrai texto página a página e salva na tabela sentences (recria do doc)."""
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT id, doc_name, file_path, lang FROM documents WHERE id=:id"),
            {"id": doc_id},
        ).mappings().first()
        if not row:
            raise ValueError(f"document {doc_id} not found")
        pages = extract_pages_text(row["file_path"])

        conn.execute(text("DELETE FROM sentences WHERE doc_id=:id"), {"id": doc_id})
        total = 0
        for p in pages:
            sents = page_to_sentences(p["text"], row["lang"] or "en")
            for i, s in enumerate(sents):
                conn.execute(text("""
                    INSERT INTO sentences (doc_id, doc_name, page, sent_idx, lang, text, lemma_text)
                    VALUES (:doc_id, :doc_name, :page, :sent_idx, :lang, :text, :lemma_text)
                """), dict(
                    doc_id=row["id"], doc_name=row["doc_name"], page=int(p["page"]), sent_idx=i,
                    lang=row["lang"] or None, text=s["text"], lemma_text=s["lemma_text"]
                ))
                total += 1
        conn.execute(text("UPDATE documents SET status='parsed' WHERE id=:id"), {"id": doc_id})
        return total

def generate_evidences_for_doc(doc_id: int) -> int:
    dct = load_dictionary()
    with engine.begin() as conn:
        doc = conn.execute(
            text("SELECT id, doc_name, lang FROM documents WHERE id=:id"),
            {"id": doc_id},
        ).mappings().first()
        if not doc:
            raise ValueError("document not found")

        sents = conn.execute(text("""
            SELECT id, page, text, lemma_text FROM sentences
            WHERE doc_id=:id ORDER BY page, sent_idx
        """), {"id": doc_id}).mappings().all()

        added = 0
        for s in sents:
            matches = match_sentence(s["text"], s["lemma_text"] or "", doc["lang"] or "en", dct)
            for m in matches:
                conn.execute(text("""
                    INSERT INTO evidences
                        (doc_name, concept_id, match_type, level, lang,
                         snippet, pattern, term_or_phrase, rule_id, score, page, method, created_at)
                    VALUES
                        (:doc_name, :concept_id, :match_type, :level, :lang,
                         :snippet, :pattern, :term_or_phrase, :rule_id, :score, :page, :method, now())
                    ON CONFLICT (doc_name, concept_id, md5(snippet)) DO NOTHING
                """), dict(
                    doc_name=doc["doc_name"],
                    concept_id=int(m["concept_id"]),
                    match_type=m["method"],
                    level=int(m["level"]),
                    lang=doc["lang"],
                    snippet=s["text"],
                    pattern=m["pattern_str"],
                    term_or_phrase=m["term_or_phrase"],
                    rule_id=m["rule_id"],
                    score=float(m["score"]) if m["score"] is not None else None,
                    page=int(s["page"]) if s["page"] is not None else None,
                    method=m["method"],
                ))
                added += 1
        return added

def process_doc(doc_id: int) -> dict:
    n_sent = extract_pdf_to_sentences(doc_id)
    n_evd  = generate_evidences_for_doc(doc_id)
    return {"doc_id": doc_id, "sentences": n_sent, "evidences": n_evd}

def process_batch(limit: int = 10) -> list[dict]:
    out = []
    with engine.begin() as conn:
        docs = conn.execute(text("""
          SELECT id FROM documents
          WHERE status IN ('uploaded','parsed') ORDER BY updated_at DESC LIMIT :lim
        """), {"lim": limit}).scalars().all()
    for did in docs:
        out.append(process_doc(did))
    return out
