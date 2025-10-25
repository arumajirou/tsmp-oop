-- 最新Runと予測の充足率
WITH latest AS (
  SELECT run_id, (config #>> '{config,horizon}')::int AS h
  FROM runs ORDER BY created_at DESC LIMIT 1
),
s AS (SELECT count(DISTINCT unique_id) n_series FROM predictions p JOIN latest l ON p.run_id=l.run_id),
c AS (SELECT count(*) n_pred FROM predictions p JOIN latest l ON p.run_id=l.run_id)
SELECT l.run_id, l.h AS horizon, s.n_series, c.n_pred,
       CASE WHEN s.n_series>0 AND l.h>0 THEN c.n_pred::float/(s.n_series*l.h) END AS ratio
FROM latest l CROSS JOIN s CROSS JOIN c;
