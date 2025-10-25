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
      SELECT unique_id, ds::text as ds, y_hat
      FROM predictions
      WHERE run_id = :rid
      ORDER BY ds
      LIMIT :lim OFFSET :off
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql), {"rid": run_id, "lim": limit, "off": offset}).mappings().all()
        return {"run_id": run_id, "count": len(rows), "items": [dict(r) for r in rows]}
