-- before creating tables (or at top)
DROP VIEW IF EXISTS predictions_view;

CREATE TABLE
  IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    alias TEXT NOT NULL,
    model_name TEXT NOT NULL,
    dataset TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'RUNNING',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    duration_sec REAL,
    config JSON
  );

CREATE TABLE
  IF NOT EXISTS predictions (
    run_id TEXT NOT NULL,
    unique_id TEXT NOT NULL,
    ds TIMESTAMP NOT NULL,
    y_hat REAL NOT NULL,
    PRIMARY KEY (run_id, unique_id, ds)
  );