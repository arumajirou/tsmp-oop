-- usage:
--   psql -d tsmodeling -v failed_days=30 -v keep_success=100 -f sql/retention.sql

WITH pruned_failed AS (
  DELETE FROM runs
  WHERE status = 'FAILED'
    AND created_at < now() - (:'failed_days' || ' days')::interval
  RETURNING run_id
),
ranked AS (
  SELECT run_id, row_number() OVER (ORDER BY created_at DESC) AS rn
  FROM runs
  WHERE status = 'SUCCEEDED'
),
pruned_success AS (
  DELETE FROM runs r
  USING ranked k
  WHERE r.run_id = k.run_id
    AND k.rn > :'keep_success'
  RETURNING r.run_id
)
SELECT 'failed_deleted' AS kind, count(*) AS n FROM pruned_failed
UNION ALL
SELECT 'success_deleted' AS kind, count(*) AS n FROM pruned_success;
