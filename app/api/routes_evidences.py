# app/api/routes_evidences.py
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional
from sqlalchemy import text
from app.db import engine

router = APIRouter()  # sem prefixo aqui; main aplica /api

@router.get("/evidences")
def list_evidences(
    doc_name: Optional[str] = None,
    concept_id: Optional[int] = None,
    lang: Optional[str] = None,
    q: Optional[str] = Query(None, description="substring no snippet (ILIKE)"),
    limit: int = 100,
    offset: int = 0,
):
    where = ["1=1"]
    params = {"limit": limit, "offset": offset}
    if doc_name:
        where.append("doc_name = :doc_name"); params["doc_name"] = doc_name
    if concept_id is not None:
        where.append("concept_id = :concept_id"); params["concept_id"] = concept_id
    if lang:
        where.append("lang = :lang"); params["lang"] = lang
    if q:
        where.append("snippet ILIKE :q"); params["q"] = f"%{q}%"

    sql = text(f"""
        SELECT id, doc_name, concept_id, match_type, level, lang, snippet, pattern, term_or_phrase
        FROM evidences
        WHERE {' AND '.join(where)}
        ORDER BY id DESC
        LIMIT :limit OFFSET :offset
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, params).mappings().all()
    return JSONResponse([dict(r) for r in rows])

@router.get("/evidences/count")
def count_evidences(
    doc_name: Optional[str] = None,
    concept_id: Optional[int] = None,
    lang: Optional[str] = None,
    q: Optional[str] = Query(None, description="substring no snippet (ILIKE)"),
):
    where = ["1=1"]; params = {}
    if doc_name:
        where.append("doc_name = :doc_name"); params["doc_name"] = doc_name
    if concept_id is not None:
        where.append("concept_id = :concept_id"); params["concept_id"] = concept_id
    if lang:
        where.append("lang = :lang"); params["lang"] = lang
    if q:
        where.append("snippet ILIKE :q"); params["q"] = f"%{q}%"

    with engine.begin() as conn:
        n = conn.execute(
            text(f"SELECT count(*) AS n FROM evidences WHERE {' AND '.join(where)}"),
            params
        ).scalar()
    return JSONResponse({"count": int(n or 0)})

@router.get("/evidences/summary")
def summary_evidences():
    with engine.begin() as conn:
        by_concept = conn.execute(text("""
            SELECT concept_id, COUNT(*) AS n
            FROM evidences
            GROUP BY concept_id
            ORDER BY n DESC
            LIMIT 50
        """)).mappings().all()

        by_doc = conn.execute(text("""
            SELECT doc_name, COUNT(*) AS n
            FROM evidences
            GROUP BY doc_name
            ORDER BY n DESC
            LIMIT 50
        """)).mappings().all()

        by_lang = conn.execute(text("""
            SELECT COALESCE(lang,'') AS lang, COUNT(*) AS n
            FROM evidences
            GROUP BY COALESCE(lang,'')
            ORDER BY n DESC
        """)).mappings().all()

    return JSONResponse({
        "by_concept": [dict(r) for r in by_concept],
        "by_doc": [dict(r) for r in by_doc],
        "by_lang": [dict(r) for r in by_lang],
    })

@router.get("/evidences/meta")
def evidences_meta():
    with engine.begin() as conn:
        doc_names = [r[0] for r in conn.execute(text(
            "SELECT DISTINCT doc_name FROM evidences ORDER BY doc_name"
        ))]
        langs = [r[0] for r in conn.execute(text(
            "SELECT DISTINCT lang FROM evidences WHERE lang IS NOT NULL ORDER BY lang"
        ))]
        concept_ids = [int(r[0]) for r in conn.execute(text(
            "SELECT DISTINCT concept_id FROM evidences ORDER BY concept_id"
        ))]
    return JSONResponse({"doc_names": doc_names, "langs": langs, "concept_ids": concept_ids})
