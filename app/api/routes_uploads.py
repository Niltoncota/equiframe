# app/api/routes_uploads.py
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import hashlib, shutil, os, time
from sqlalchemy import text
from app.db import engine

router = APIRouter()

DATA_DIR   = Path(os.getenv("DATA_DIR", "/data"))
INPUT_DIR  = Path(os.getenv("DICT_INPUT_DIR", DATA_DIR / "input"))
LOADED_DIR = INPUT_DIR / "loaded"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
LOADED_DIR.mkdir(parents=True, exist_ok=True)

def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _move_to_loaded(path: Path) -> Path:
    ts = time.strftime("%Y%m%d-%H%M%S")
    dst = LOADED_DIR / f"{ts}__{path.name}"
    shutil.move(str(path), str(dst))
    return dst

@router.post("/upload/pdf")
@router.post("/upload/pdf/")
async def upload_pdf(file: UploadFile = File(...), doc_name: str = Form(...), lang: str | None = Form(None)):
    if file.content_type not in {"application/pdf"}:
        raise HTTPException(415, f"Tipo inválido: {file.content_type}")
    safe = file.filename.replace("..", "_").replace("/", "_")
    tmp = INPUT_DIR / safe
    with tmp.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    digest = _sha256(tmp)

    with engine.begin() as conn:
        # UPSERT por sha256 (tabela 'documents' já criada por você)
        doc_id = conn.execute(text("""
            INSERT INTO documents (doc_name, file_path, sha256, lang, status)
            VALUES (:doc_name, :file_path, :sha256, :lang, 'uploaded')
            ON CONFLICT (sha256) DO UPDATE
              SET doc_name = EXCLUDED.doc_name,
                  file_path = EXCLUDED.file_path,
                  lang      = COALESCE(EXCLUDED.lang, documents.lang),
                  status    = 'uploaded',
                  updated_at = now()
            RETURNING id
        """), dict(doc_name=doc_name, file_path=str(tmp), sha256=digest, lang=lang)).scalar_one()

    final_path = _move_to_loaded(tmp)
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE documents
               SET file_path = :final_path,
                   updated_at = now()
             WHERE id = :doc_id
        """), {"final_path": str(final_path), "doc_id": int(doc_id)})
    
    return JSONResponse({"ok": True, "doc_id": int(doc_id), "sha256": digest, "saved_to": str(final_path)})
