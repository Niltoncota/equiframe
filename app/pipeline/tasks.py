# app/pipeline/tasks.py
import os
import pandas as pd
from celery import shared_task
from celery.utils.log import get_task_logger
from app.db import insert_evidences_df, engine
from sqlalchemy import text
# legado: sua pipeline atual que exporta evidences.csv/jsonl
from .pilot import run_pilot  # mantém compatibilidade

logger = get_task_logger(__name__)

OUTPUT_CSV = "/data/output/evidences.csv"
OUTPUT_JSONL = "/data/output/evidences.jsonl"

# escolhe a implementação da pipeline: 'legacy' (default) ou 'v2'
PIPELINE_IMPL = os.getenv("PIPELINE_IMPL", "legacy").lower()
REINDEX_AFTER = os.getenv("REINDEX_AFTER_BATCH", "false").lower() == "true"
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "10"))

def _insert_df_with_defaults(df: pd.DataFrame) -> int:
    """
    Padroniza colunas do CSV legado para o schema atual de evidences
    e usa a função de inserção existente (ON CONFLICT DO NOTHING).
    """
    # colunas mínimas esperadas pelo banco
    required = [
        "doc_name", "concept_id", "match_type", "level", "lang",
        "snippet", "pattern", "term_or_phrase",
        "rule_id", "score", "page", "method"
    ]
    # defaults para o legado
    defaults = {
        "match_type": "term",
        "level": 1,
        "lang": None,
        "pattern": None,
        "term_or_phrase": None,
        "rule_id": None,
        "score": None,
        "page": None,
        "method": "lexical",
    }
    for k, v in defaults.items():
        if k not in df.columns:
            df[k] = v

    # tipos básicos
    if "concept_id" in df.columns:
        df["concept_id"] = pd.to_numeric(df["concept_id"], errors="coerce").astype("Int64")
    if "level" in df.columns:
        df["level"] = pd.to_numeric(df["level"], errors="coerce").fillna(1).astype(int)
    if "page" in df.columns:
        df["page"] = pd.to_numeric(df["page"], errors="coerce").astype("Int64")
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")

    # ordena/filtra colunas
    for col in required:
        if col not in df.columns:
            df[col] = None
    df = df[required]

    # dedupe lógico igual ao seu índice (doc_name, concept_id, md5(snippet))
    df = df.drop_duplicates(subset=["doc_name", "concept_id", "snippet"], keep="first")
    if df.empty:
        return 0

    insert_evidences_df(df)  # já faz ON CONFLICT DO NOTHING
    return int(df.shape[0])

@shared_task(name="app.search.reindex")
def reindex_meili():
    from app.search.indexer import index_all
    return index_all()

@shared_task(name="app.pipeline.tasks.process_doc")
def process_doc(doc_id: int):
    if PIPELINE_IMPL != "v2":
        return {"impl": "legacy", "error": "process_doc só disponível com PIPELINE_IMPL=v2"}

    logger.info("process_doc START doc_id=%s", doc_id)
    from .v2 import process_doc as process_doc_v2
    try:
        res = process_doc_v2(doc_id)  # ex.: {'doc_id': 1, 'sentences': 415, 'evidences': 403}

        # (opcional) marca no DB o status e as contagens
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE documents
                   SET status = 'processed',
                       sentence_count = :sentences,
                       evidence_count = :evidences,
                       processed_at = now()
                 WHERE id = :doc_id
            """), res)

        logger.info("process_doc DONE doc_id=%s | %s", doc_id, res)
        if REINDEX_AFTER:
            reindex_meili.apply_async()
        return {"impl": "v2", **res}

    except Exception as e:
        logger.exception("process_doc ERROR doc_id=%s", doc_id)
        # (opcional) grava erro no documento
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE documents
                   SET status = 'error',
                       last_error = :err,
                       updated_at = now()
                 WHERE id = :doc_id
            """), {"err": str(e), "doc_id": doc_id})
        raise

@shared_task(name="app.pipeline.tasks.process_batch")
def process_batch():
    """
    LEGADO (default): roda run_pilot() -> lê /data/output/evidences.csv -> insere.
    V2: extrai, tokeniza, casa com dicionário e insere direto no Postgres.
    """
    if PIPELINE_IMPL == "legacy":
        res = run_pilot()  # mantém seu fluxo atual
        inserted = 0
        if os.path.exists(OUTPUT_CSV):
            try:
                df = pd.read_csv(OUTPUT_CSV)
                if not df.empty:
                    inserted = _insert_df_with_defaults(df)
            except Exception:
                inserted = 0
        out = {
            "impl": "legacy",
            "output_csv": OUTPUT_CSV,
            "output_jsonl": OUTPUT_JSONL,
            "inserted": inserted,
        }
        if isinstance(res, dict):
            out.update(res)
    else:
        from .v2 import process_batch as process_batch_v2
        results = process_batch_v2(limit=BATCH_LIMIT)
        out = {"impl": "v2", "results": results}

    if REINDEX_AFTER:
        reindex_meili.apply_async()
    return out
