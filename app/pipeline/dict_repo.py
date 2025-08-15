from sqlalchemy import text
from app.db import engine

def load_dictionary():
    """Carrega dicionário do Postgres em estruturas rápidas."""
    with engine.begin() as conn:
        # termos
        lt = conn.execute(text("""
            SELECT concept_id, lang, term, COALESCE(lemma, term) AS lemma,
                   COALESCE(weight,1.0) AS weight, COALESCE(priority,1) AS priority
            FROM lexicon_terms
        """)).mappings().all()
        # key phrases
        kp = conn.execute(text("""
            SELECT concept_id, lang, phrase,
                   COALESCE(weight,1.0) AS weight, COALESCE(priority,1) AS priority
            FROM key_phrases
        """)).mappings().all()
        # regras (level_type é TEXTO!)
        rules = conn.execute(text("""
            SELECT id, lang, level_type, pattern, negation_pattern,
                   COALESCE(priority,1) AS priority
            FROM pattern_rules
        """)).mappings().all()

    # índice por idioma
    def norm_lang(l): return (l or "").lower() or None
    terms_by_lang = {}
    for r in lt:
        lang = norm_lang(r["lang"])
        terms_by_lang.setdefault(lang, []).append(r)

    phrases_by_lang = {}
    for r in kp:
        lang = norm_lang(r["lang"])
        phrases_by_lang.setdefault(lang, []).append(r)

    import re
    rules_by_lang = {}
    for r in rules:
        lang = norm_lang(r["lang"])
        comp = re.compile(r["pattern"], flags=re.I|re.M)
        neg  = re.compile(r["negation_pattern"], flags=re.I|re.M) if r["negation_pattern"] else None
        rules_by_lang.setdefault(lang, []).append({
            "id": r["id"], "level_type": r["level_type"], "pattern": comp,
            "neg": neg, "priority": r["priority"]
        })

    return {
        "terms_by_lang": terms_by_lang,
        "phrases_by_lang": phrases_by_lang,
        "rules_by_lang": rules_by_lang,
    }
