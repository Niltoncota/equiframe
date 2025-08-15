# EquiFrame NLP — Policy Analysis & Evidence Extraction

**Version:** 1.0  
**Date:** 15 Aug 2025  
**Author:** Nilton Cota  
**Contact:** niltoncota@gmail.com  

---

## 📝 Executive Summary

*EquiFrame NLP is an offline, explainable AI tool for analyzing health policy documents. It scores the coverage of 21 human rights Core Concepts and 12 Vulnerable Groups (VGs) defined by the EquiFrame framework, producing evidence snippets and coverage/quality indices per document.*

The system uses a hybrid lexicon-based and semantic search approach (BM25 + embeddings) to detect mentions, classify commitment levels (1–4), and generate explainable outputs for human review.  
It works fully offline, supports Portuguese and English, and is containerized for reproducibility.

---

## 📊 System Flow Diagram

```ascii
┌────────────┐      ┌─────────────┐     ┌───────────────┐
│  PDF/Word  │ ──▶  │   Ingest    │ ──▶ │ NLP Processing │
└────────────┘      │ (Celery)    │     │ (spaCy, regex) │
                    └─────┬───────┘     └──────┬────────┘
                          ▼                  ▼
                   ┌─────────────┐     ┌───────────────┐
                   │  Matching   │     │  VG Detection │
                   │ (Lexicon,   │     │ (Terms, Regex)│
                   │ BM25, Emb.) │     └──────┬────────┘
                   └─────┬───────┘            ▼
                         ▼             ┌───────────────┐
                  ┌─────────────┐     │ Indices &      │
                  │ Evidences   │     │ Rankings       │
                  │ (Postgres & │     └──────┬────────┘
                  │ Meilisearch)│            ▼
                  └─────┬───────┘     ┌───────────────┐
                        ▼             │ Dashboard     │
                   ┌─────────────┐    │ (Streamlit)   │
                   │ API (FastAPI)│──▶ │ Upload, Search│
                   └─────────────┘    │ Compare, Export│
                                       └───────────────┘
🚀 Features Implemented
Ingestion: Upload PDF/Word → text extraction → sentence segmentation.

Matching: Exact match + BM25 + multilingual embeddings.

Classification: Commitment levels 1–4 (mention → monitoring).

Storage: PostgreSQL (structured data) + Meilisearch (full-text search).

Evidence Output: Snippets, matched term, rule triggered.

Indices: Coverage %, Quality % (≥3), VG coverage, overall ranking.

Dashboard: Search, upload, stats, export CSV/PDF.

Async Processing: Celery + Redis.

🛠 Tech Stack
Core: Python 3.10+, FastAPI, SQLAlchemy

NLP: spaCy (EN/PT), sentence-transformers, regex, unidecode

Search: Meilisearch v1.7

DB & Queue: PostgreSQL 15, Redis, Celery

UI: Streamlit dashboard

PDF: pdfminer.six, pdf2image, Tesseract OCR (planned)

Orchestration: Docker Compose

📂 Architecture Overview
app/api: FastAPI endpoints (routes_*)

app/pipeline: Celery tasks, PDF/NLP/matcher modules

app/dashboard_app.py: Streamlit UI

app/dictionary: CSV loaders for concepts, terms, phrases, patterns

/data: Mounted volume for input/output data

docker-compose.yml: Multi-service setup

📜 Processing Workflow
Upload document via API/dashboard → saved to /data/input/.

Celery ingestion task extracts text, splits into sentences, detects language.

Matcher searches for Core Concepts & Vulnerable Groups using:

Lexicon terms & key phrases

Pattern rules for classification 1–4

BM25 & semantic embeddings for recall

Evidences stored in PostgreSQL & indexed in Meilisearch.

Indices computed with SQL functions (refresh_*).

Dashboard/API used for querying, comparing, exporting results.

🗄 Data Model (Key Tables)
documents: Metadata for PDFs/Word files.

sentences: Segmented text with language & lemma.

concepts, lexicon_terms, key_phrases, pattern_rules

vulnerable_groups, doc_vg_sentence_hits, doc_vg_mentions

hits, evidences, summaries

doc_concept_scores, doc_equiframe_indices

doc_cc_vg_matrix: CC×VG co-occurrence

🔌 API Highlights
Reindex:

POST /api/vg/reindex

POST /api/cc/reindex?doc_id={id}

POST /api/all/reindex?doc_id={id}

VG:

GET /api/vg/stats

GET /api/vg/mentions?doc_id={id}

GET /api/vg/terms?...

CC:

GET /api/cc/stats

GET /api/docs/{doc_id}/indices

GET /api/docs/{doc_id}/cc-scores

GET /api/docs/{doc_id}/cc-vg-matrix

Search:

GET /api/search?q=...

📊 Dashboard Features
Upload & process documents

Search evidences by term, doc, concept, language

View VG & CC statistics

Export results (CSV, PDF highlights)

Trigger reindex & dictionary sync

📌 Current Status
✅ Core pipeline operational (80% complete)
⚠️ Pending:

OCR fallback for scanned PDFs

Improved 1–4 scoring heuristics (negation, tie-break)

Dashboard doc comparison & CRUD dictionary

Gold set creation & evaluation metrics

API response standardization & auth

Refresh performance tuning

Enhanced PDF export

🏁 How to Run Locally
bash
Copy
Edit
# 1. Clone repository
git clone https://github.com/Niltoncota/equiframe.git
cd equiframe

# 2. Create .env (adapt ports/paths)
cp .env.example .env

# 3. Start services
docker compose up -d

# 4. Access:
# API: http://localhost:8000/docs
# Dashboard: http://localhost:8501
📧 Contact
For issues, questions or contributions, please contact: niltoncota@gmail.com