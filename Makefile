# ---- CONFIG ----
API ?= http://localhost:8000/api
PDF ?= ./amostra.pdf               # ajuste ao chamar: make upload-pdf PDF=./leis/lei123.pdf
DOC ?= Lei-amostra                 # nome amigável
LANG ?= pt

# ---- D1–D2: DICIONÁRIO ----
# 1) Sincroniza CSVs encontrados em /data e move p/ /data/input/loaded
sync-dict:
    @curl -s -X POST "$(API)/dictionary/sync?reindex=false" | jq .

# 2) Stats das tabelas de dicionário + evidences
dict-stats:
    @curl -s "$(API)/dictionary/stats" | jq .

# 3) Reindex Meili (se quiser acoplar aqui)
reindex:
    @curl -s -X POST "$(API)/search/reindex" | jq .

# ---- PDFs ----
upload-pdf:
    @curl -s -F "file=@$(PDF)" -F "doc_name=$(DOC)" -F "lang=$(LANG)" \
        "$(API)/upload/pdf" | jq .

# ---- Smoke de API (Evidences/Search) ----
smoke:    
	@echo ">> /health"; curl -s "http://localhost:8000/health" | jq .
    @echo ">> /api/evidences/meta"; curl -s "$(API)/evidences/meta" | jq .
    @echo ">> /api/evidences?limit=3"; curl -s "$(API)/evidences?limit=3" | jq .
    @echo ">> /api/search?q=privacidade&limit=3"; curl -s "$(API)/search?q=privacidade&limit=3" | jq .
    @echo ">> /api/search/facets?q=privacidade"; curl -s "$(API)/search/facets?q=privacidade" | jq .

# ---- DENTRO DOS CONTAINERS (alternativa) ----
# Use se preferir rodar via docker compose exec
in-api-reindex:
    @docker compose exec -T api python -m app.search.indexer

in-worker-process:
    @docker compose exec -T worker python - <<'PY'
from app.pipeline.tasks import process_batch
r = process_batch.delay()
print(r.get(timeout=1800))
PY

.PHONY: sync-dict dict-stats reindex upload-pdf smoke in-api-reindex in-worker-process
