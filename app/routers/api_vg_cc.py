#app/app/routers/api_vg_cc.py:
from fastapi import APIRouter
from typing import Any, Dict, List, Optional
import os, traceback
import asyncpg

router = APIRouter(prefix="/api", tags=["metrics"])

# ---- conexões --------------------------------------------------------------
async def get_conn() -> asyncpg.Connection:
    dsn = os.getenv('DATABASE_URL')
    if dsn:
        if dsn.startswith('postgresql+'):
            dsn = 'postgresql://' + dsn.split('://', 1)[1]
        if dsn.startswith('postgres+'):
            dsn = 'postgres://' + dsn.split('://', 1)[1]
    else:
        user = os.getenv('POSTGRES_USER', 'postgres')
        pwd  = os.getenv('POSTGRES_PASSWORD', '')
        db   = os.getenv('POSTGRES_DB', 'postgres')
        host = os.getenv('POSTGRES_HOST', 'db')
        port = os.getenv('POSTGRES_PORT', '5432')
        dsn  = f'postgres://{user}:{pwd}@{host}:{port}/{db}'
    return await asyncpg.connect(dsn)

# ---- helpers ---------------------------------------------------------------
async def fetch_dict(conn: asyncpg.Connection, query: str, *args) -> Dict[str, Any]:
    rec = await conn.fetchrow(query, *args)
    return dict(rec) if rec else {}

async def fetch_list(conn: asyncpg.Connection, query: str, *args) -> List[Dict[str, Any]]:
    rows = await conn.fetch(query, *args)
    return [dict(r) for r in rows]

def ok(data: Any = None) -> Dict[str, Any]:
    return {"ok": True, "data": data} if data is not None else {"ok": True}

def err(e: Exception) -> Dict[str, Any]:
    return {"ok": False, "error": str(e), "trace": traceback.format_exc()}

# ---- reindex endpoints -----------------------------------------------------
@router.post('/vg/reindex')
async def vg_reindex():
    conn = await get_conn()
    try:
        await conn.execute('SELECT refresh_vg_mentions();')
        return ok()
    except Exception as e:
        return err(e)
    finally:
        await conn.close()

@router.post('/cc/reindex')
async def cc_reindex(doc_id: Optional[int] = None):
    conn = await get_conn()
    try:
        await conn.execute('SELECT refresh_core_concepts($1);', doc_id)
        return ok()
    except Exception as e:
        return err(e)
    finally:
        await conn.close()

@router.post('/all/reindex')
async def all_reindex():
    conn = await get_conn()
    try:
        await conn.execute('SELECT refresh_vg_mentions();')
        await conn.execute('SELECT refresh_core_concepts();')
        return ok()
    except Exception as e:
        return err(e)
    finally:
        await conn.close()

# ---- stats endpoints -------------------------------------------------------
@router.get('/vg/stats')
async def vg_stats():
    conn = await get_conn()
    try:
        totals = await fetch_dict(conn, '''
            SELECT
              (SELECT COUNT(*) FROM vulnerable_groups) AS vulnerable_groups,
              (SELECT COUNT(*) FROM vg_lexicon_terms)  AS vg_lexicon_terms,
              (SELECT COUNT(*) FROM doc_vg_mentions)   AS doc_vg_mentions
        ''')
        by_lang = await fetch_list(conn, '''
            SELECT lang, COUNT(*) AS n
            FROM vg_lexicon_terms GROUP BY 1 ORDER BY 1
        ''')
        by_source = await fetch_list(conn, '''
            SELECT source_ref, priority, COUNT(*) AS n
            FROM vg_lexicon_terms GROUP BY 1,2 ORDER BY 1,2
        ''')
        top_docs = await fetch_list(conn, '''
            SELECT doc_id, SUM(mention_cnt) AS total
            FROM doc_vg_mentions
            GROUP BY doc_id
            ORDER BY total DESC
            LIMIT 20
        ''')
        return {"totals": totals, "by_lang": by_lang, "by_source": by_source, "top_docs": top_docs}
    except Exception as e:
        return err(e)
    finally:
        await conn.close()

@router.get('/vg/mentions')
async def vg_mentions(doc_id: Optional[int] = None):
    conn = await get_conn()
    try:
        if doc_id is None:
            q = ('SELECT doc_id, SUM(mention_cnt) AS total '
                 'FROM vw_vg_mentions_summary GROUP BY doc_id '
                 'ORDER BY total DESC LIMIT 100')
            return await fetch_list(conn, q)
        q = '''
            SELECT m.vg_id, g.name_pt, m.mention_cnt
            FROM vw_vg_mentions_summary AS m
            JOIN vulnerable_groups AS g ON g.id = m.vg_id
            WHERE m.doc_id = $1
            ORDER BY m.mention_cnt DESC, m.vg_id
        '''
        return await fetch_list(conn, q, int(doc_id))
    except Exception as e:
        return err(e)
    finally:
        await conn.close()

@router.get('/cc/stats')
async def cc_stats():
    conn = await get_conn()
    try:
        totals = await fetch_dict(conn, '''
            SELECT
              (SELECT COUNT(*) FROM doc_concept_scores)    AS doc_concept_scores,
              (SELECT COUNT(*) FROM doc_equiframe_indices) AS doc_equiframe_indices
        ''')
        top_docs = await fetch_list(conn, '''
            SELECT doc_id, cc_covered, vg_covered,
                   ROUND((pct_cc_covered*100)::numeric,1)    AS pct_cc_cov,
                   ROUND((pct_cc_quality_3p*100)::numeric,1) AS pct_cc_q3
            FROM doc_equiframe_indices
            ORDER BY cc_covered DESC, doc_id LIMIT 20
        ''')
        by_concept = await fetch_list(conn, '''
            SELECT c.id AS concept_id, COUNT(d.doc_id) AS docs_com_evidencia
            FROM concepts c
            LEFT JOIN doc_concept_scores d
              ON d.concept_id = c.id AND d.evidence_cnt > 0
            GROUP BY c.id ORDER BY c.id
        ''')
        return {"totals": totals, "top_docs": top_docs, "by_concept": by_concept}
    except Exception as e:
        return err(e)
    finally:
        await conn.close()

# ---- /vg/terms -------------------------------------------------------------
@router.get('/vg/terms')
async def vg_terms(
    vg_id: Optional[int] = None,
    lang: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    conn = await get_conn()
    try:
        sql = '''
            SELECT
              v.vg_id,
              g.name_en,
              g.name_pt,
              v.lang,
              v.term,
              v.lemma,
              v.weight,
              v.source_ref,
              v.priority
            FROM vg_lexicon_terms v
            JOIN vulnerable_groups g ON g.id = v.vg_id
            WHERE ($1::int  IS NULL OR v.vg_id = $1)
              AND ($2::text IS NULL OR v.lang  = $2)
              AND ($3::text IS NULL OR v.term ILIKE '%' || $3 || '%')
            ORDER BY v.vg_id, v.lang, v.term
            LIMIT $4 OFFSET $5
        '''
        rows = await conn.fetch(sql, vg_id, lang, q, limit, offset)
        return [dict(r) for r in rows]
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()}
    finally:
        await conn.close()
