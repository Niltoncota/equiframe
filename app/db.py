# app/db.py
import os
from sqlalchemy import create_engine, text

# Usa DATABASE_URL se existir; caso contrário, monta a partir do compose
DATABASE_URL = os.getenv("DATABASE_URL") or \
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@db:5432/{os.getenv('POSTGRES_DB')}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def insert_evidences_df(df):
    """Insere em evidences com ON CONFLICT usando índice (doc_name, concept_id, md5(snippet))."""
    cols = ["doc_name","concept_id","match_type","level","lang","snippet","pattern","term_or_phrase"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    rows = df[cols].to_dict(orient="records")

    sql = text("""
        INSERT INTO evidences
          (doc_name, concept_id, match_type, level, lang, snippet, pattern, term_or_phrase)
        VALUES
          (:doc_name, :concept_id, :match_type, :level, :lang, :snippet, :pattern, :term_or_phrase)
        ON CONFLICT (doc_name, concept_id, md5(snippet)) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
