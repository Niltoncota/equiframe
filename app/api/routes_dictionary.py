# app/api/routes_dictionary.py
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import text
from app.db import engine
from app.dictionary.loader import sync_inputs
from typing import List
import os, shutil, time
from pathlib import Path

router = APIRouter()  # prefixo /api vem do main.py

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

@router.post("/dictionary/sync")
def dictionary_sync(reindex: bool = True):
    """
    Lê CSVs em /data, faz UPSERT nas tabelas e move cada arquivo para /data/input/loaded.
    Opcionalmente reindexa o Meili ao final (reindex=True).
    """
    sync = sync_inputs()
    reidx = None
    if reindex:
        # reindex só se você quiser amarrar isso aqui; pode deixar False no frontend/CI
        from app.search.indexer import index_all
        reidx = index_all()
    return JSONResponse({"ok": True, "sync": sync, "reindex": reidx})

@router.get("/dictionary/stats")
def dictionary_stats():
    with engine.begin() as conn:
        stats = {
            "concepts":       int(conn.execute(text("SELECT COUNT(*) FROM concepts")).scalar() or 0),
            "lexicon_terms":  int(conn.execute(text("SELECT COUNT(*) FROM lexicon_terms")).scalar() or 0),
            "key_phrases":    int(conn.execute(text("SELECT COUNT(*) FROM key_phrases")).scalar() or 0),
            "pattern_rules":  int(conn.execute(text("SELECT COUNT(*) FROM pattern_rules")).scalar() or 0),
            "evidences":      int(conn.execute(text("SELECT COUNT(*) FROM evidences")).scalar() or 0),
        }
    return JSONResponse(stats)

@router.post("/dictionary/upload")
async def dictionary_upload(files: List[UploadFile] = File(...)):
    """
    Recebe 1..N CSVs e salva em /data (sem processar).
    Depois rode /api/dictionary/sync para aplicar UPSERT e mover para /data/input/loaded.
    """
    saved = []
    for f in files:
        name = (f.filename or "file.csv").replace("..", "_").replace("/", "_")
        if not name.lower().endswith(".csv"):
            raise HTTPException(415, f"Somente CSV: {name}")
        dst = DATA_DIR / name
        with dst.open("wb") as w:
            shutil.copyfileobj(f.file, w)
        saved.append(str(dst))
    return {"ok": True, "saved": saved, "hint": "Agora chame POST /api/dictionary/sync"}