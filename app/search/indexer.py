# app/search/indexer.py

from meilisearch import Client, errors as meili_err
from app.db import engine
from sqlalchemy import text
import os, time, math

MEILI_URL = os.getenv("MEILI_URL", "http://meili:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY")
INDEX_NAME = "evidences"

def _task_uid(task):
    # aceita dict OU objeto (TaskInfo)
    if isinstance(task, dict):
        return task.get("taskUid") or task.get("uid") or task.get("id")
    for name in ("taskUid", "uid", "id", "task_uid", "task_id"):
        if hasattr(task, name):
            return getattr(task, name)
    return None

def _wait(client: Client, task, timeout_ms=180_000, interval_ms=200):
    uid = _task_uid(task)
    if uid is None:
        return {"status": "unknown", "taskUid": None}
    try:
        res = client.wait_for_task(uid, timeout_in_ms=timeout_ms, interval_in_ms=interval_ms)
        status = res.get("status") if isinstance(res, dict) else getattr(res, "status", None)
        return {"status": status, "taskUid": uid}
    except meili_err.MeilisearchTimeoutError:
        return {"status": "timeout", "taskUid": uid}

def _clean_doc(d):
    def to_text(v):
        if v is None: return ""
        if isinstance(v, float) and math.isnan(v): return ""
        return str(v)
    for k in ("doc_name","match_type","lang","snippet","pattern","term_or_phrase"):
        d[k] = to_text(d.get(k))
    for k in ("id","concept_id","level"):
        v = d.get(k)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            d[k] = 0
        else:
            try: d[k] = int(v)
            except Exception: d[k] = 0
    return d

def index_all(batch_size=200):
    client = Client(MEILI_URL, MEILI_KEY)

    # (re)cria índice com primaryKey=id
    try:
        client.index(INDEX_NAME).delete()
    except Exception:
        pass
    r = client.create_index(INDEX_NAME, {"primaryKey": "id"})
    _wait(client, r)

    idx = client.index(INDEX_NAME)
    r = idx.update_settings({
        "searchableAttributes": ["snippet", "pattern", "term_or_phrase", "doc_name"],
        "filterableAttributes": ["doc_name", "concept_id", "lang"],
    })
    _wait(client, r)

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, doc_name, concept_id, match_type, level, lang, snippet, pattern, term_or_phrase
            FROM evidences
        """)).mappings().all()

    docs = [_clean_doc(dict(r)) for r in rows]
    if not docs:
        return {"sent": 0, "status": "ok", "note": "no rows"}

    sent = 0
    for i in range(0, len(docs), batch_size):
        chunk = docs[i:i+batch_size]
        t = idx.add_documents(chunk)  # upsert por id
        _wait(client, t)
        sent += len(chunk)

    return {"sent": sent, "status": "ok"}

if __name__ == "__main__":
    print(index_all())
