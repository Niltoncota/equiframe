#main.py:
from fastapi import FastAPI
from pydantic import BaseModel
from app.api import routes_evidences, routes_tasks, routes_search, routes_docs
from app.api.routes_dictionary import router as dictionary_router
from app.api.routes_uploads import router as uploads_router
from app.routers import api_vg_cc

app = FastAPI(title="Equiframe API")

# APLICA UM ÚNICO PREFIXO GLOBAL /api
app.include_router(routes_evidences.router, prefix="/api")
app.include_router(routes_search.router,    prefix="/api")
app.include_router(routes_tasks.router,     prefix="/api")
app.include_router(dictionary_router,       prefix="/api")
app.include_router(uploads_router,          prefix="/api")
app.include_router(routes_docs.router,       prefix="/api")
app.include_router(api_vg_cc.router)
class Health(BaseModel):
    status: str

@app.get("/health", response_model=Health)
def health():
    return {"status": "ok"}
    

