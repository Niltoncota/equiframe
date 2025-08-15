import os, re, csv, json, subprocess, shlex
from pathlib import Path

DATA_DIR = Path("/data")
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_CONCEPTS = DATA_DIR / "equiframe_concepts_v2.csv"
CSV_TERMS    = DATA_DIR / "equiframe_lexicon_terms_v2.csv"
CSV_PHRASES  = DATA_DIR / "equiframe_key_phrases_v2.csv"
CSV_PATTERNS = DATA_DIR / "equiframe_pattern_rules_v2.csv"

def extract_text(path: Path) -> str:
    p = str(path)
    if path.suffix.lower() == ".pdf":
        cmd = f'pdftotext -layout -enc UTF-8 {shlex.quote(p)} -'
        try:
            return subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, timeout=180).decode("utf-8", errors="ignore")
        except subprocess.CalledProcessError:
            return ""
    elif path.suffix.lower() in [".doc", ".docx", ".odt", ".rtf"]:
        out_txt = path.with_suffix(".txt")
        cmd = f'libreoffice --headless --convert-to txt:Text --outdir {shlex.quote(str(path.parent))} {shlex.quote(p)}'
        try:
            subprocess.check_call(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, timeout=180)
            return out_txt.read_text(encoding="utf-8", errors="ignore") if out_txt.exists() else ""
        except subprocess.CalledProcessError:
            return ""
        finally:
            if out_txt.exists():
                try: out_txt.unlink()
                except Exception: pass
    else:
        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return ""

def load_csv_rows(path: Path):
    import pandas as pd
    if not path.exists(): return []
    df = pd.read_csv(path)
    return df.to_dict(orient="records")

def compile_patterns(rows, field="pattern"):
    pats = []
    for r in rows:
        pat = r.get(field)
        if not isinstance(pat, str) or not pat.strip(): continue
        try:
            rx = re.compile(pat, flags=re.IGNORECASE|re.UNICODE|re.MULTILINE)
            pats.append((rx, r))
        except re.error:
            continue
    return pats

def sentence_iter(text: str):
    for s in re.split(r'(?<=[\.\!\?])\s+|\n{2,}', text):
        s2 = s.strip()
        if s2: yield s2

def run_pilot():
    terms   = load_csv_rows(CSV_TERMS)
    phrases = load_csv_rows(CSV_PHRASES)
    patterns= load_csv_rows(CSV_PATTERNS)
    comp_patterns = compile_patterns(patterns)

    out_csv = OUTPUT_DIR / "evidences.csv"
    out_json = OUTPUT_DIR / "evidences.jsonl"
    with open(out_csv, "w", newline="", encoding="utf-8") as fout, open(out_json, "w", encoding="utf-8") as jout:
        w = csv.writer(fout)
        w.writerow(["doc_name","concept_id","match_type","level","lang","snippet","pattern","term_or_phrase"])

        files = []
        if (INPUT_DIR).exists():
            for ext in ("*.pdf","*.doc","*.docx","*.odt","*.rtf","*.txt"):
                files.extend(INPUT_DIR.glob(ext))

        for f in files:
            text = extract_text(f)
            if not text: continue
            for sent in sentence_iter(text):
                s_norm = sent.lower()

                for r in terms:
                    t = str(r.get("term","")).strip().lower()
                    if t and t in s_norm:
                        w.writerow([f.name, r.get("concept_id"), "term", 1, r.get("lang"), sent[:500], "", t])
                        jout.write(json.dumps({"doc":f.name,"concept_id":r.get("concept_id"),"type":"term","level":1,"lang":r.get("lang"),"text":sent})+"\n")

                for r in phrases:
                    p = str(r.get("phrase","")).strip().lower()
                    if p and p in s_norm:
                        w.writerow([f.name, r.get("concept_id"), "phrase", 2, r.get("lang"), sent[:500], "", p])
                        jout.write(json.dumps({"doc":f.name,"concept_id":r.get("concept_id"),"type":"phrase","level":2,"lang":r.get("lang"),"text":sent})+"\n")

                for rx, r in comp_patterns:
                    if rx.search(sent):
                        lvl = int(r.get("level") or 3)
                        patt = r.get("pattern","")
                        w.writerow([f.name, r.get("concept_id"), "pattern", lvl, r.get("lang") or "", sent[:500], patt, ""])
                        jout.write(json.dumps({"doc":f.name,"concept_id":r.get("concept_id"),"type":"pattern","level":lvl,"lang":r.get("lang") or "","text":sent,"pattern":patt})+"\n")

    return {"processed": len(files), "output_csv": str(out_csv), "output_jsonl": str(out_json)}

if __name__ == "__main__":
    print(run_pilot())
