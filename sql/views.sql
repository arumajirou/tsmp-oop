CREATE OR REPLACE VIEW run_summary AS
SELECT
  run_id, alias, model_name, dataset, status,
  duration_sec, created_at, updated_at,
  json_extract_path_text(config::json, 'config', 'horizon') as horizon
FROM runs
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW predictions_per_run AS
SELECT r.run_id, r.alias, r.model_name, r.created_at,
       count(p.*) AS n_predictions
FROM runs r
LEFT JOIN predictions p ON p.run_id = r.run_id
GROUP BY r.run_id, r.alias, r.model_name, r.created_at
ORDER BY r.created_at DESC;
