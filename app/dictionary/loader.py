# app/dictionary/loader.py
import os, math, time, shutil, hashlib
import pandas as pd
from sqlalchemy import text
from app.db import engine

# pastas (altere via env se quiser)
DATA_DIR = os.getenv("DATA_DIR", "/data")
INPUT_DIR = os.getenv("DICT_INPUT_DIR", os.path.join(DATA_DIR, "input"))
LOADED_DIR = os.path.join(INPUT_DIR, "loaded")


os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(LOADED_DIR, exist_ok=True)

def _to_int(v, default=None):
    try:
        if v is None: return default
        if isinstance(v, float) and math.isnan(v): return default
        return int(v)
    except Exception:
        return default

def _to_float(v, default=None):
    try:
        if v is None: return default
        if isinstance(v, float) and math.isnan(v): return default
        return float(v)
    except Exception:
        return default

def _to_str(v, default=None):
    if v is None: return default
    if isinstance(v, float) and math.isnan(v): return default
    s = str(v).strip()
    return s if s else default

def _hash_to_int(s: str) -> int:
    # id determinístico se precisar (quando não vier id na regra)
    h = int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16)
    return h % 2147483647

def _move_to_loaded(path: str):
    base = os.path.basename(path)
    ts = time.strftime("%Y%m%d-%H%M%S")
    dst = os.path.join(LOADED_DIR, f"{ts}__{base}")
    shutil.move(path, dst)
    return dst

def _read_csv_safe(path: str) -> pd.DataFrame:
    # tolera BOM, ; ou , como separador, não transforma string vazia em NaN
    return pd.read_csv(
        path,
        encoding="utf-8-sig",
        engine="python",
        sep=None,               # autodetect
        keep_default_na=False,  # strings vazias continuam vazias
    )


# -------- UPSERTS (assumem índices/constraints) --------
SQL_UPSERT_CONCEPTS = text("""
INSERT INTO concepts (id, concept_name_en, concept_name_pt, definition_en, definition_pt, name)
VALUES (:id, :concept_name_en, :concept_name_pt, :definition_en, :definition_pt, :name)
ON CONFLICT (id) DO UPDATE
SET concept_name_en = EXCLUDED.concept_name_en,
    concept_name_pt = EXCLUDED.concept_name_pt,
    definition_en   = EXCLUDED.definition_en,
    definition_pt   = EXCLUDED.definition_pt,
    name            = EXCLUDED.name;
""")

SQL_UPSERT_LEXICON = text("""
INSERT INTO lexicon_terms (concept_id, lang, term, lemma, weight, source_ref, priority)
VALUES (:concept_id, :lang, :term, :lemma, :weight, :source_ref, :priority)
ON CONFLICT (concept_id, lang, term) DO UPDATE
SET lemma      = COALESCE(EXCLUDED.lemma,      lexicon_terms.lemma),
    weight     = COALESCE(EXCLUDED.weight,     lexicon_terms.weight),
    source_ref = COALESCE(EXCLUDED.source_ref, lexicon_terms.source_ref),
    priority   = COALESCE(EXCLUDED.priority,   lexicon_terms.priority);
""")

SQL_UPSERT_KEYPHR = text("""
INSERT INTO key_phrases (concept_id, lang, phrase, weight, source_ref, priority)
VALUES (:concept_id, :lang, :phrase, :weight, :source_ref, :priority)
ON CONFLICT (concept_id, lang, phrase) DO UPDATE
SET weight     = COALESCE(EXCLUDED.weight,     key_phrases.weight),
    source_ref = COALESCE(EXCLUDED.source_ref, key_phrases.source_ref),
    priority   = COALESCE(EXCLUDED.priority,   key_phrases.priority);
""")

SQL_UPSERT_RULES = text("""
INSERT INTO pattern_rules (id, lang, level_type, pattern, negation_pattern, examples, source_ref, priority)
VALUES (:id, :lang, :level_type, :pattern, :negation_pattern, :examples, :source_ref, :priority)
ON CONFLICT (id) DO UPDATE
SET lang             = EXCLUDED.lang,
    level_type       = EXCLUDED.level_type,
    pattern          = EXCLUDED.pattern,
    negation_pattern = COALESCE(EXCLUDED.negation_pattern, pattern_rules.negation_pattern),
    examples         = COALESCE(EXCLUDED.examples,         pattern_rules.examples),
    source_ref       = COALESCE(EXCLUDED.source_ref,       pattern_rules.source_ref),
    priority         = COALESCE(EXCLUDED.priority,         pattern_rules.priority);
""")

# -------- carregadores --------
def _load_concepts(df: pd.DataFrame):
    # aceita: (id|concept_id), (name|concept_name), description(opcional)
    rows = []
    for _, r in df.iterrows():
        cid  = r.get("id", r.get("concept_id"))
        if cid is None or (isinstance(cid, float) and pd.isna(cid)):
            # gera id determinístico se vier só o nome
            name_for_id = _to_str(r.get("name", r.get("concept_name")))
            if name_for_id:
                cid = _hash_to_int(f"concept:{name_for_id}")
        name = r.get("name", r.get("concept_name"))
        rows.append({
            "id": _to_int(cid),
            "concept_name_en": _to_str(r.get("concept_name_en")),
            "concept_name_pt": _to_str(r.get("concept_name_pt")),
            "definition_en": _to_str(r.get("definition_en")),
            "definition_pt": _to_str(r.get("definition_pt")),
            "name": _to_str(r.get("concept_name_en"))  
        })
    rows = [x for x in rows if x["id"] is not None and x["concept_name_en"]]
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(SQL_UPSERT_CONCEPTS, rows)
    return len(rows)
    
def _load_lexicon_terms(df: pd.DataFrame):
    # aceita "source" → source_ref; priority default 1
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "concept_id": _to_int(r.get("concept_id")),
            "lang": (_to_str(r.get("lang")) or "").lower() or None,
            "term": _to_str(r.get("term")),
            "lemma": _to_str(r.get("lemma")) or _to_str(r.get("term")),
            "weight": _to_float(r.get("weight"), 1.0),
            "source_ref": _to_str(r.get("source_ref", r.get("source"))),
            "priority": _to_int(r.get("priority"), 1),
        })
    rows = [x for x in rows if x["concept_id"] is not None and x["lang"] and x["term"]]
    with engine.begin() as conn:
        conn.execute(SQL_UPSERT_LEXICON, rows)
    return len(rows)

def _load_key_phrases(df: pd.DataFrame):
    # aceita "source" → source_ref; priority default 1
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "concept_id": _to_int(r.get("concept_id")),
            "lang": (_to_str(r.get("lang")) or "").lower() or None,
            "phrase": _to_str(r.get("phrase")),
            "weight": _to_float(r.get("weight"), 1.0),
            "source_ref": _to_str(r.get("source_ref", r.get("source"))),
            "priority": _to_int(r.get("priority"), 1),
        })
    rows = [x for x in rows if x["concept_id"] is not None and x["lang"] and x["phrase"]]
    with engine.begin() as conn:
        conn.execute(SQL_UPSERT_KEYPHR, rows)
    return len(rows)

def _load_pattern_rules(df: pd.DataFrame):
    rows = []
    for _, r in df.iterrows():
        lang       = (_to_str(r.get("lang")) or "").lower() or None
        level_type = _to_str(r.get("level_type"))  # **texto**, não converter para int
        pattern    = _to_str(r.get("pattern"))
        rid        = r.get("id")
        rid        = _to_int(rid) if rid is not None else None
        if rid is None and lang and level_type and pattern:
            rid = _hash_to_int(f"{lang}|{level_type}|{pattern}")

        rows.append({
            "id":               rid,
            "lang":             lang,
            "level_type":       level_type,
            "pattern":          pattern,
            "negation_pattern": _to_str(r.get("negation_pattern")),
            "examples":         _to_str(r.get("examples")),
            "source_ref":       _to_str(r.get("source_ref", r.get("source"))),
            "priority":         _to_int(r.get("priority"), 1),
        })

    rows = [x for x in rows if x["id"] is not None and x["lang"] and x["level_type"] and x["pattern"]]
    if not rows: return 0
    with engine.begin() as conn:
        conn.execute(SQL_UPSERT_RULES, rows)
    return len(rows)
    
# -------- orquestrador --------
def _classify_csv(path: str) -> str|None:
    name = os.path.basename(path).lower()
    if name.endswith(".csv"):
        if "concept" in name:        return "concepts"
        if "lexicon" in name:        return "lexicon_terms"
        if "key_phrase" in name or "keyphrase" in name or "key-phrase" in name:
                                      return "key_phrases"
        if "pattern_rule" in name or "pattern" in name:
                                      return "pattern_rules"
    return None

def _load_single_csv(path: str) -> dict:
    kind = _classify_csv(path)
    if not kind:
        return {"file": path, "skipped": True, "reason": "unknown_csv_type"}
    df = pd.read_csv(path)
    n = 0
    if kind == "concepts":
        n = _load_concepts(df)
    elif kind == "lexicon_terms":
        n = _load_lexicon_terms(df)
    elif kind == "key_phrases":
        n = _load_key_phrases(df)
    elif kind == "pattern_rules":
        n = _load_pattern_rules(df)
    moved_to = _move_to_loaded(path)
    return {"file": path, "type": kind, "upserts": n, "moved_to": moved_to}

def sync_inputs() -> dict:
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR)
         if f.lower().endswith(".csv") and os.path.isfile(os.path.join(DATA_DIR, f))]    
    results, total = [], 0
    for f in sorted(files):
        try:
            r = _load_single_csv(f)
            results.append(r)
            total += r.get("upserts", 0)
        except Exception as e:
            results.append({"file": f, "error": str(e)})
    return {"processed_files": len(files), "total_upserts": total, "results": results}
