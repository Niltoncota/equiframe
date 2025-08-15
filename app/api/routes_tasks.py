# app/api/routes_tasks.py
from fastapi import APIRouter
from celery.result import AsyncResult
from app.pipeline.tasks import process_batch, process_doc, reindex_meili

router = APIRouter()

@router.post("/tasks/process")
def trigger_process_batch():
    r = process_batch.delay()
    return {"task_id": r.id}

@router.post("/tasks/process_doc/{doc_id}")
def trigger_process_doc(doc_id: int):
    """
    Dispara processamento do documento (V2 se PIPELINE_IMPL=v2).
    """
    r = process_doc.delay(doc_id)
    return {"task_id": r.id, "doc_id": doc_id}

@router.post("/tasks/reindex")
def trigger_reindex():
    r = reindex_meili.delay()
    return {"task_id": r.id}

@router.get("/tasks/{task_id}")
def task_status(task_id: str):
    ar = AsyncResult(task_id)
    payload = {"task_id": task_id, "state": ar.state}
    if ar.successful():
        payload["result"] = ar.result
    elif ar.failed():
        payload["error"] = str(ar.result)
    return payload