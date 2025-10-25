# src/tsmp/api/app.py
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict
import os
from sqlalchemy import create_engine, text

app = FastAPI(title="tsmp-oop")

DSN = os.getenv("POSTGRES_DSN", "postgresql:///tsmodeling")
engine = create_engine(DSN, pool_pre_ping=True)

class RunRow(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    run_id: str
    alias: str
    model_name: str
    dataset: str
    status: str
    duration_sec: float | None = None
    created_at: str
    updated_at: str
    horizon: int | None = None

@app.get("/runs/latest", response_model=RunRow)
def latest_run():
    sql = """
      SELECT r.run_id, r.alias, r.model_name, r.dataset, r.status, r.duration_sec,
             r.created_at::text, r.updated_at::text,
             COALESCE((r.config #>> '{config,horizon}')::int, NULL) AS horizon
      FROM runs r
      ORDER BY created_at DESC
      LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql)).mappings().first()
        if not row: raise HTTPException(404, "no runs")
        return dict(row)

@app.get("/predictions")
def predictions(run_id: str = Query(...), limit: int = 1000, offset: int = 0):
    sql = """
      SELECT unique_id, to_char(ds AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ds, y_hat
      FROM predictions
      WHERE run_id = :rid
      ORDER BY ds
      LIMIT :lim OFFSET :off
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql), {"rid": run_id, "lim": limit, "off": offset}).mappings().all()
        return {"run_id": run_id, "count": len(rows), "items": [dict(r) for r in rows]}

from fastapi import status
import math

KPI_DURATION_P95_MS = int(os.getenv("KPI_DURATION_P95_MS", "5000"))
KPI_PREDICTIONS_RATIO = float(os.getenv("KPI_PREDICTIONS_RATIO", "1.0"))

@app.get("/health")
def health():
    # 1) duration p95
    sql_p95 = "SELECT COALESCE(percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_sec), 0) AS p95 FROM runs"
    # 2) 最新 run の予測充足率（= 実際件数 / (ユニーク系列数 * horizon)）
    sql_ratio = """
      WITH latest AS (
        SELECT run_id, (config #>> '{config,horizon}')::int AS horizon
        FROM runs ORDER BY created_at DESC LIMIT 1
      ),
      s AS (SELECT count(DISTINCT p.unique_id) AS n_series
            FROM predictions p JOIN latest l ON p.run_id = l.run_id),
      c AS (SELECT count(*) AS n_pred
            FROM predictions p JOIN latest l ON p.run_id = l.run_id)
      SELECT l.run_id, l.horizon, s.n_series, c.n_pred,
             CASE WHEN s.n_series>0 AND l.horizon>0
                  THEN c.n_pred::float / (s.n_series * l.horizon)
                  ELSE NULL END AS ratio
      FROM latest l CROSS JOIN s CROSS JOIN c
    """
    with engine.begin() as conn:
        p95 = float(conn.execute(text(sql_p95)).scalar_one())
        r = conn.execute(text(sql_ratio)).mappings().first()
        ratio = (float(r["ratio"]) if r and r["ratio"] is not None else None)
        latest_run = (r["run_id"] if r else None)

    ok_p95 = (p95 * 1000.0) <= KPI_DURATION_P95_MS  # sec→ms
    ok_ratio = (ratio is None) or (ratio >= KPI_PREDICTIONS_RATIO)

    overall = bool(ok_p95 and ok_ratio)
    status_code = status.HTTP_200_OK if overall else status.HTTP_503_SERVICE_UNAVAILABLE
    return {
        "ok": overall,
        "checks": {
            "duration_p95_sec": p95,
            "threshold_ms": KPI_DURATION_P95_MS,
            "predictions_ratio": ratio,
            "ratio_threshold": KPI_PREDICTIONS_RATIO,
            "latest_run_id": latest_run
        }
    }, status_code


from fastapi import status
import math

KPI_DURATION_P95_MS = int(os.getenv("KPI_DURATION_P95_MS", "5000"))
KPI_PREDICTIONS_RATIO = float(os.getenv("KPI_PREDICTIONS_RATIO", "1.0"))

@app.get("/health")
def health():
    # 1) duration p95
    sql_p95 = "SELECT COALESCE(percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_sec), 0) AS p95 FROM runs"
    # 2) 最新 run の予測充足率（= 実際件数 / (ユニーク系列数 * horizon)）
    sql_ratio = """
      WITH latest AS (
        SELECT run_id, (config #>> '{config,horizon}')::int AS horizon
        FROM runs ORDER BY created_at DESC LIMIT 1
      ),
      s AS (SELECT count(DISTINCT p.unique_id) AS n_series
            FROM predictions p JOIN latest l ON p.run_id = l.run_id),
      c AS (SELECT count(*) AS n_pred
            FROM predictions p JOIN latest l ON p.run_id = l.run_id)
      SELECT l.run_id, l.horizon, s.n_series, c.n_pred,
             CASE WHEN s.n_series>0 AND l.horizon>0
                  THEN c.n_pred::float / (s.n_series * l.horizon)
                  ELSE NULL END AS ratio
      FROM latest l CROSS JOIN s CROSS JOIN c
    """
    with engine.begin() as conn:
        p95 = float(conn.execute(text(sql_p95)).scalar_one())
        r = conn.execute(text(sql_ratio)).mappings().first()
        ratio = (float(r["ratio"]) if r and r["ratio"] is not None else None)
        latest_run = (r["run_id"] if r else None)

    ok_p95 = (p95 * 1000.0) <= KPI_DURATION_P95_MS  # sec→ms
    ok_ratio = (ratio is None) or (ratio >= KPI_PREDICTIONS_RATIO)

    overall = bool(ok_p95 and ok_ratio)
    status_code = status.HTTP_200_OK if overall else status.HTTP_503_SERVICE_UNAVAILABLE
    return {
        "ok": overall,
        "checks": {
            "duration_p95_sec": p95,
            "threshold_ms": KPI_DURATION_P95_MS,
            "predictions_ratio": ratio,
            "ratio_threshold": KPI_PREDICTIONS_RATIO,
            "latest_run_id": latest_run
        }
    }, status_code
