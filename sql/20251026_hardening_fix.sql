-- sql/20251026_hardening_fix.sql
-- 1) y_hat の NaN/Inf 防止（real型なので cast または論理式で）
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'predictions_y_hat_finite_chk'
  ) THEN
    ALTER TABLE predictions
      ADD CONSTRAINT predictions_y_hat_finite_chk
      CHECK (
        -- NaN は自分自身と等しくない性質を利用
        y_hat = y_hat
        AND y_hat < 'infinity'::real
        AND y_hat > '-infinity'::real
      );
  END IF;
END $$;

-- 2) view 依存を外してから jsonb 化 → 再作成
DROP VIEW IF EXISTS run_summary;

ALTER TABLE runs
  ALTER COLUMN config TYPE jsonb USING config::jsonb;

-- horizon を int として再生成
CREATE OR REPLACE VIEW run_summary AS
SELECT
  run_id, alias, model_name, dataset, status,
  duration_sec, created_at, updated_at,
  (config #>> '{config,horizon}')::int AS horizon
FROM runs
ORDER BY created_at DESC;
