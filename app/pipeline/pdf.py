import fitz  # PyMuPDF
from pathlib import Path

def extract_pages_text(pdf_path: str) -> list[dict]:
    """Retorna [{'page': 1, 'text': '...'}, ...]  (1-indexed)."""
    out = []
    with fitz.open(pdf_path) as doc:
        for i, page in enumerate(doc, start=1):
            text = page.get_text("text") or ""
            out.append({"page": i, "text": text})
    return out
