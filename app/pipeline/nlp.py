from functools import lru_cache
import spacy

@lru_cache(maxsize=2)
def get_nlp(lang: str):
    if lang and lang.lower().startswith("pt"):
        return spacy.load("pt_core_news_sm", disable=["ner"])
    return spacy.load("en_core_web_sm", disable=["ner"])

def page_to_sentences(text: str, lang: str) -> list[dict]:
    """Corta em sentenças + lemas; retorna [{'text','lemma_text'}]."""
    nlp = get_nlp(lang or "en")
    doc = nlp(text)
    out = []
    for sent in doc.sents:
        s = sent.text.strip()
        if not s:
            continue
        lemma = " ".join([t.lemma_.lower() for t in sent if not t.is_space])
        out.append({"text": s, "lemma_text": lemma})
    return out
