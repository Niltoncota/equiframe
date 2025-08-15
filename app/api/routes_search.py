# app/api/routes_search.py
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import Optional, List
from meilisearch import Client
import os, json, logging


router = APIRouter()  # sem prefixo aqui; main aplica /api

MEILI_URL = os.getenv("MEILI_URL", "http://meili:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY")
INDEX_NAME = "evidences"

def get_client() -> Client:
    return Client(MEILI_URL, MEILI_KEY)

def _cap(n: Optional[int], lo: int = 1, hi: int = 200) -> int:
    """Clampa limites para evitar exageros/erros."""
    try:
        n = int(n or 0)
    except Exception:
        n = lo
    return max(lo, min(n, hi))

def _build_filter(doc_name: Optional[str], concept_id: Optional[int], lang: Optional[str]) -> Optional[str]:
    parts: List[str] = []
    if doc_name:
        parts.append(f'doc_name = "{doc_name}"')
    if concept_id is not None:
        parts.append(f"concept_id = {int(concept_id)}")
    if lang:
        parts.append(f'lang = "{lang}"')
    return " AND ".join(parts) if parts else None

@router.get("/search")
@router.get("/search/")
def search_evidences(
    q: str = Query(..., description="Texto a buscar (snippet/pattern/term_or_phrase)"),
    doc_name: Optional[str] = None,
    concept_id: Optional[int] = None,
    lang: Optional[str] = None,
    limit: int = 20,
):
    try:
        client = get_client()
        filt = _build_filter(doc_name, concept_id, lang)
        payload = {
            "limit": _cap(limit),
            "filter": filt,
            # opcional: highlight para mostrar <mark> no dashboard
            "attributesToHighlight": ["snippet"],
            "highlightPreTag": "<mark>",
            "highlightPostTag": "</mark>",
        }
        logging.info("Meili search | q=%r | filter=%r | limit=%d", q, filt, payload["limit"])
        res = client.index(INDEX_NAME).search(q, payload)
        return JSONResponse(content=jsonable_encoder(res))
    except Exception as e:
        logging.exception("search_evidences failed")
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.get("/search/facets")
@router.get("/search/facets/")
def search_facets(q: str = "", limit: int = 0):
    """Retorna facetas; limit=0 evita hits."""
    try:
        client = get_client()
        payload = {
            "limit": 0 if int(limit or 0) == 0 else _cap(limit),
            "facets": ["doc_name", "concept_id", "lang"],
            "attributesToRetrieve": [],
        }
        logging.info("Meili facets | q=%r | limit=%s", q, payload["limit"])
        res = client.index(INDEX_NAME).search(q, payload)
        return JSONResponse(content=jsonable_encoder(res))
    except Exception as e:
        logging.exception("search_facets failed")
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.post("/search/reindex")
def reindex_all():
    from app.search.indexer import index_all
    return index_all()
