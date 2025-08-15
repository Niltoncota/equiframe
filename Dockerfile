FROM python:3.11-slim

ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Pacotes de sistema para OCR/PDF
RUN apt-get update && apt-get install -y --no-install-recommends \
	sudo locate which nano  \
	wget curl ca-certificates \
    build-essential gcc g++ make libpq-dev pkg-config \
    tesseract-ocr tesseract-ocr-por tesseract-ocr-eng \
    poppler-utils ghostscript libmagic1 \
    libreoffice wget curl unzip fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

 

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Modelos spaCy
RUN python -m spacy download pt_core_news_sm && \
    python -m spacy download en_core_web_sm

COPY . .
RUN useradd -m app && chown -R app /app
USER app
