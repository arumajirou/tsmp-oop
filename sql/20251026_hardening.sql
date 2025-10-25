-- 予測の時刻はタイムゾーン込みで保持
ALTER TABLE predictions
  ALTER COLUMN ds TYPE timestamptz USING (ds AT TIME ZONE 'UTC');

-- 参照整合性：予測は必ず既存のrunにぶら下がる
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'predictions_run_fk'
  ) THEN
    ALTER TABLE predictions
      ADD CONSTRAINT predictions_run_fk
      FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE;
  END IF;
END $$;

-- 異常値混入を抑止（NaN/Inf禁止）
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'predictions_y_hat_finite_chk'
  ) THEN
    ALTER TABLE predictions
      ADD CONSTRAINT predictions_y_hat_finite_chk
      CHECK (isfinite(y_hat));
  END IF;
END $$;

-- ステータスを実質Enum化
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'runs_status_chk'
  ) THEN
    ALTER TABLE runs
      ADD CONSTRAINT runs_status_chk
      CHECK (status IN ('RUNNING','SUCCEEDED','FAILED'));
  END IF;
END $$;

-- 実運用向けインデックス
CREATE INDEX IF NOT EXISTS idx_predictions_run ON predictions(run_id);
CREATE INDEX IF NOT EXISTS idx_predictions_uid_ds ON predictions(unique_id, ds);

-- JSON -> jsonb で将来のGIN検索に備える（型だけ）
ALTER TABLE runs
  ALTER COLUMN config TYPE jsonb USING config::jsonb;
