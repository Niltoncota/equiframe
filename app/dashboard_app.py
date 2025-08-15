# app/dashboard_app.py
import os
import io
import requests
import streamlit as st
import pandas as pd

API_URL = os.getenv("API_URL", "http://api:8000")

st.set_page_config(page_title="Equiframe", layout="wide")
st.title("Equiframe Dashboard")

# -------- Health -------------------------------------------------------------
try:
    r = requests.get(f"{API_URL}/health", timeout=3)
    r.raise_for_status()
    st.success(f"API OK: {r.json()}")
except Exception as e:
    st.error(f"API indisponível em {API_URL}: {e}")

st.markdown("---")

# ===== Meta (doc_names, langs, concept_ids) =================================
@st.cache_data(ttl=30)
def load_meta():
    r = requests.get(f"{API_URL}/api/evidences/meta", timeout=5)
    r.raise_for_status()
    return r.json()

meta = {}
try:
    meta = load_meta()
except Exception as e:
    st.warning(f"Não foi possível carregar meta: {e}")

# ----------------------------------------------------------------------------
# BUSCA (Meili)
# ----------------------------------------------------------------------------
st.subheader("🔎 Buscar evidências (Meili)")

s_doc_names = [""] + meta.get("doc_names", [])
s_langs     = [""] + meta.get("langs", [])
s_cids      = [""] + [str(x) for x in meta.get("concept_ids", [])]

q = st.text_input("Texto para buscar", "")
c1, c2, c3, c4 = st.columns(4)
with c1:
    s_doc_name = st.selectbox("doc_name", options=s_doc_names, index=0, key="search_doc_name")
with c2:
    s_concept_id = st.selectbox("concept_id", options=s_cids, index=0, key="search_cid")
with c3:
    s_lang = st.selectbox("lang", options=s_langs, index=0, key="search_lang")
with c4:
    s_limit = st.number_input("limit", value=20, min_value=1, max_value=200, step=1, key="search_limit")

colA, colB = st.columns([1,1])
with colA:
    do_search = st.button("🔍 Buscar", use_container_width=True)
with colB:
    if st.button("♻️ Reindexar Meili (full)", use_container_width=True):
        try:
            r = requests.post(f"{API_URL}/api/search/reindex", timeout=60)
            r.raise_for_status()
            st.success(f"Reindex: {r.json()}")
        except Exception as e:
            st.error(f"Falha no reindex: {e}")

if do_search and q.strip():
    try:
        params = {"q": q.strip(), "limit": int(s_limit)}
        if s_doc_name:  params["doc_name"] = s_doc_name
        if s_concept_id: params["concept_id"] = int(s_concept_id)
        if s_lang:      params["lang"] = s_lang

        # hits
        r = requests.get(f"{API_URL}/api/search", params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        hits = data.get("hits", [])

        left, right = st.columns([2, 1])

        with left:
            st.write(f"**{data.get('estimatedTotalHits', 0)}** resultados")
            if hits:
                rows = []
                for h in hits:
                    fmt = h.get("_formatted") or {}
                    rows.append({
                        "id":            h.get("id"),
                        "doc_name":      h.get("doc_name"),
                        "concept_id":    h.get("concept_id"),
                        "lang":          h.get("lang"),
                        "match_type":    h.get("match_type"),
                        "level":         h.get("level"),
                        "term_or_phrase":h.get("term_or_phrase"),
                        "pattern":       h.get("pattern"),
                        "snippet":       fmt.get("snippet") or h.get("snippet"),
                    })
                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True, hide_index=True)

                with st.expander("Ver trechos com destaque"):
                    for r_ in rows:
                        header = f"**{r_.get('doc_name','')}** — concept_id={r_.get('concept_id','')}, lang={r_.get('lang','')}, id={r_.get('id','')}"
                        st.markdown(header)
                        st.markdown(r_.get("snippet",""), unsafe_allow_html=True)
                        st.markdown("---")
            else:
                st.warning("Nenhum resultado.")

        # facets (apenas por q)
        with right:
            fr = requests.get(f"{API_URL}/api/search/facets", params={"q": q.strip()}, timeout=10)
            if fr.ok:
                facets = fr.json().get("facetDistribution", {})
                st.subheader("Facets")
                for fname in ("doc_name", "concept_id", "lang"):
                    dist = facets.get(fname)
                    if dist:
                        st.markdown(f"**{fname}**")
                        st.json(dist)
            else:
                st.info("Sem facets.")
    except Exception as e:
        st.error(f"Erro na busca: {e}")
elif not q.strip():
    st.info("Digite um texto e clique em **Buscar** para iniciar.")

st.markdown("---")

# ----------------------------------------------------------------------------
# LISTAGEM / EXPORT de evidences (Postgres)
# ----------------------------------------------------------------------------
st.header("📄 Evidences (Postgres)")

f_doc_name = st.selectbox("doc_name", ["(todos)"] + sorted(meta.get("doc_names", [])), key="ev_doc_name")
f_lang     = st.selectbox("lang", ["(todas)"] + sorted(meta.get("langs", [])), key="ev_lang")
f_cid_opt  = st.selectbox("concept_id", ["(todos)"] + [str(x) for x in sorted(meta.get("concept_ids", []))], key="ev_cid")
f_cid      = None if f_cid_opt in (None, "", "(todos)") else int(f_cid_opt)
f_q_sub    = st.text_input("filtrar por substring (ILIKE)", "", key="ev_q")
f_limit    = st.number_input("limit", value=100, min_value=1, max_value=2000, step=50, key="ev_limit")

params = {"limit": int(f_limit)}
if f_doc_name and f_doc_name != "(todos)":
    params["doc_name"] = f_doc_name
if f_lang and f_lang != "(todas)":
    params["lang"] = f_lang
if f_cid is not None:
    params["concept_id"] = f_cid
if f_q_sub.strip():
    params["q"] = f_q_sub.strip()

if st.button("Buscar evidences", type="primary"):
    try:
        r = requests.get(f"{API_URL}/api/evidences", params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            st.error(f"Resposta inesperada da API: {data}")
        else:
            st.success(f"{len(data)} linhas")
            df = pd.DataFrame(data)
            st.dataframe(df, use_container_width=True, hide_index=True)

            csv_bytes = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Exportar CSV",
                data=csv_bytes,
                file_name="evidences_export.csv",
                mime="text/csv"
            )
    except Exception as e:
        st.error(f"Erro ao buscar evidences: {e}")

st.markdown("---")

# ----------------------------------------------------------------------------
# 📚 Dictionary — Stats + Sync + Upload CSV
# ----------------------------------------------------------------------------
st.subheader("📚 Dictionary")

col_d1, col_d2 = st.columns([1,1])

with col_d1:
    try:
        ds = requests.get(f"{API_URL}/api/dictionary/stats", timeout=5).json()
        st.metric("Concepts",      ds.get("concepts", 0))
        st.metric("Lexicon terms", ds.get("lexicon_terms", 0))
        st.metric("Key phrases",   ds.get("key_phrases", 0))
        st.metric("Pattern rules", ds.get("pattern_rules", 0))
        st.metric("Evidences",     ds.get("evidences", 0))
    except Exception as e:
        st.error(f"Falha ao carregar stats do dicionário: {e}")

with col_d2:
    if st.button("🔄 Sync dictionary (CSV → DB) + Reindex", use_container_width=True):
        try:
            res = requests.post(f"{API_URL}/api/dictionary/sync", timeout=300).json()
            st.success("Sync OK")
            with st.expander("Detalhes do sync"):
                st.json(res.get("sync", {}))
            if "reindex" in res and res["reindex"]:
                st.info(f"Reindex: {res['reindex']}")
        except Exception as e:
            st.error(f"Falha no sync: {e}")

st.subheader("📚 Dicionário — Upload de CSVs")
with st.form("dict_uploader", clear_on_submit=True):
    csvs = st.file_uploader(
        "Selecione 1 ou mais CSVs (concepts, lexicon_terms, key_phrases, pattern_rules)",
        type=["csv"], accept_multiple_files=True
    )
    c1, c2 = st.columns([1,1])
    with c1:
        sent_csv = st.form_submit_button("⬆️ Enviar CSVs para /data")
    with c2:
        sent_and_sync = st.form_submit_button("⬆️ Enviar + 🔄 Sync + ♻️ Reindex")

def _post_csvs(files):
    up_files = [("files", (f.name, f.getvalue(), "text/csv")) for f in files]
    r = requests.post(f"{API_URL}/api/dictionary/upload", files=up_files, timeout=180)
    r.raise_for_status()
    return r.json()

if sent_csv or sent_and_sync:
    if not csvs:
        st.warning("Selecione ao menos um CSV.")
    else:
        try:
            res = _post_csvs(csvs)
            st.success(f"Upload OK: {len(res.get('saved', []))} arquivo(s).")
            with st.expander("Arquivos salvos"):
                st.json(res)
            if sent_and_sync:
                r2 = requests.post(f"{API_URL}/api/dictionary/sync", timeout=300)
                r2.raise_for_status()
                st.info("Sync concluído.")
                with st.expander("Resultado do sync"):
                    st.json(r2.json())
        except Exception as e:
            st.error(f"Falha no upload/sync do dicionário: {e}")

st.markdown("---")

# ----------------------------------------------------------------------------
# 📤 Upload de PDF e Processamento (V2)
# ----------------------------------------------------------------------------
st.header("📤 Upload de PDF e Processamento (V2)")

up_col1, up_col2 = st.columns([2,1])

with up_col1:
    pdf = st.file_uploader("Selecione um PDF", type=["pdf"])
    up_doc_name = st.text_input("doc_name", value="")
    up_lang = st.selectbox("lang", options=["", "en", "pt"], index=0)
with up_col2:
    if st.button("Enviar PDF", use_container_width=True, type="primary"):
        if not pdf or not up_doc_name.strip():
            st.error("Informe um PDF e um doc_name.")
        else:
            try:
                files = {"file": (pdf.name, io.BytesIO(pdf.read()), "application/pdf")}
                data = {"doc_name": up_doc_name.strip()}
                if up_lang.strip():
                    data["lang"] = up_lang.strip()
                r = requests.post(f"{API_URL}/api/upload/pdf", files=files, data=data, timeout=120)
                r.raise_for_status()
                res = r.json()
                st.session_state["last_doc_id"] = res.get("doc_id")
                st.success(f"Upload OK: doc_id={res.get('doc_id')}")
                st.json(res)
            except Exception as e:
                st.error(f"Falha no upload: {e}")

# Disparo + Status
proc_col1, proc_col2 = st.columns([1,1])
with proc_col1:
    doc_id_def = st.session_state.get("last_doc_id", 0)
    p_doc_id = st.number_input("doc_id para processar (V2)", value=int(doc_id_def or 0), min_value=0, step=1)
    if st.button("🚀 Processar doc (V2)", use_container_width=True):
        if int(p_doc_id) <= 0:
            st.error("Informe um doc_id válido (>0).")
        else:
            try:
                r = requests.post(f"{API_URL}/api/tasks/process_doc/{int(p_doc_id)}", timeout=30)
                r.raise_for_status()
                res = r.json()
                st.session_state["last_task_id"] = res.get("task_id")
                st.success(f"Task enviada: {res}")
            except Exception as e:
                st.error(f"Falha ao enviar task: {e}")

with proc_col2:
    last_task = st.session_state.get("last_task_id", "")
    task_id_in = st.text_input("task_id para consultar status", value=str(last_task or ""))
    if st.button("🔎 Ver status da task", use_container_width=True):
        if not task_id_in.strip():
            st.error("Informe um task_id.")
        else:
            try:
                r = requests.get(f"{API_URL}/api/tasks/{task_id_in.strip()}", timeout=10)
                r.raise_for_status()
                st.json(r.json())
            except Exception as e:
                st.error(f"Falha ao consultar status: {e}")

st.caption("Developed by niltoncota@gmail.com")
