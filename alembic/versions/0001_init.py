"""init schema (runs, predictions, indexes)"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '0001_init'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # PostgreSQL/SQLite の両対応（IF NOT EXISTS で冪等）
    op.execute("""
    CREATE TABLE IF NOT EXISTS runs (
      run_id TEXT PRIMARY KEY,
      alias TEXT,
      model_name TEXT,
      dataset TEXT,
      status TEXT,
      duration_sec DOUBLE PRECISION,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      config JSONB
    );
    """)
    op.execute("""
    CREATE TABLE IF NOT EXISTS predictions (
      run_id TEXT NOT NULL,
      unique_id TEXT NOT NULL,
      ds TIMESTAMPTZ NOT NULL,
      yhat DOUBLE PRECISION,
      y_hat DOUBLE PRECISION,
      value DOUBLE PRECISION
    );
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_predictions_run_ds_uid
      ON predictions(run_id, ds, unique_id);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_runs_created_at
      ON runs((created_at));
    """)
    op.execute("""
    DO $$
    BEGIN
      IF to_regclass('idx_runs_config_gin') IS NULL THEN
        CREATE INDEX idx_runs_config_gin ON runs USING GIN (config);
      END IF;
    EXCEPTION WHEN others THEN
      -- SQLite 等 GIN 不可環境は無視
      NULL;
    END $$;
    """)

def downgrade():
    -- 重要: 本番は DROP 慎重。ここでは no-op （必要なら明示的に DROP を記述）
    pass
