CREATE OR REPLACE VIEW vw_vg_mentions_summary AS
SELECT m.doc_id, m.vg_id, SUM(m.mention_cnt)::int AS mention_cnt
FROM doc_vg_mentions m
GROUP BY m.doc_id, m.vg_id;
