from rapidfuzz import fuzz
import re
from typing import Optional

LEVEL_MAP = {"mention":1, "promise":2, "action":3, "monitor":4, "negation":1}

def match_sentence(sent_text: str, lemma_text: str, lang: str, dct) -> list[dict]:
    """
    Retorna lista de evidences candidates:
    {concept_id, level, rule_id, pattern_str, term_or_phrase, score, method}
    """
    out = []
    lang = (lang or "").lower() or None
    terms = dct["terms_by_lang"].get(lang, [])
    kps   = dct["phrases_by_lang"].get(lang, [])
    rules = dct["rules_by_lang"].get(lang, [])

    text_l = sent_text.lower()

    # 1) Lexicon terms by lemma exact-ish
    for r in terms:
        # exato no lemma_text (rápido) + fallback fuzzy leve no texto bruto
        hit = (f" {r['lemma'].lower()} " in f" {lemma_text} ") or \
              (fuzz.partial_ratio(r["term"].lower(), text_l) >= 90)
        if hit:
            out.append({
                "concept_id": r["concept_id"],
                "level": 1,
                "rule_id": None,
                "pattern_str": None,
                "term_or_phrase": r["term"],
                "score": 0.5 * r["weight"] + 0.1 * r["priority"],
                "method": "lexical"
            })

    # 2) Key phrases (string match case-insensitive)
    for r in kps:
        phrase = r["phrase"].lower()
        if phrase and phrase in text_l:
            out.append({
                "concept_id": r["concept_id"],
                "level": 1,
                "rule_id": None,
                "pattern_str": phrase,
                "term_or_phrase": r["phrase"],
                "score": 1.0 * r["weight"] + 0.2 * r["priority"],
                "method": "lexical"
            })

    # 3) Pattern rules (regex + negação)
    for r in rules:
        if r["pattern"].search(sent_text):
            neg_block = r["neg"].search(sent_text) if r["neg"] else False
            lvl = LEVEL_MAP.get(r["level_type"], 1)
            score = 1.5 + 0.3 * r["priority"]
            if neg_block and r["level_type"] != "negation":
                score *= 0.2  # penaliza se houver padrão de negação
            out.append({
                "concept_id": None,  # nivel via pattern pode ser genérico; conceito vem dos termos/frases na mesma sentença
                "level": lvl,
                "rule_id": r["id"],
                "pattern_str": r["pattern"].pattern,
                "term_or_phrase": None,
                "score": score,
                "method": "lexical"
            })

    # Consolidação simples: se houver pattern + termo/frase, propaga conceito do termo/frase com maior score
    if any(c["rule_id"] for c in out) and any(c.get("concept_id") for c in out):
        best_concept: Optional[int] = None
        best_score = -1
        for c in out:
            if c.get("concept_id") and c["score"] > best_score:
                best_concept, best_score = c["concept_id"], c["score"]
        for c in out:
            if c.get("concept_id") is None and c["rule_id"] is not None:
                c["concept_id"] = best_concept

    # remove entradas sem concept_id (quando só casa pattern genérico)
    out = [c for c in out if c.get("concept_id") is not None]
    # agrupa por concept_id pegando maior score/level e explicação principal
    agg = {}
    for c in out:
        k = c["concept_id"]
        if k not in agg or c["score"] > agg[k]["score"]:
            agg[k] = c
    return list(agg.values())
