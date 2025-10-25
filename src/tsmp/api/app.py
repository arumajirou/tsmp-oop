# src/tsmp/api/app.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict
import os
from sqlalchemy import create_engine, text
from fastapi.responses import JSONResponse
from datetime import datetime, timezone

app = FastAPI(title="tsmp-oop")

app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])

DSN = os.getenv("POSTGRES_DSN", "postgresql:///tsmodeling")
engine = create_engine(DSN, pool_pre_ping=True)

def _mlflow_hint(run_name: str) -> dict | None:
    """run_name(=DBのrun_id)で MLflow UI の検索URLを組む。
    前提:
      - MLFLOW_UI_BASE (例: http://127.0.0.1:5000)
      - MLFLOW_EXPERIMENT_NAME (既定 'tsmp-oop')
      - mlflow が import 可能なら experiment_id を解決して実験内検索URLを返す
      - 無ければ UI ベースだけ返す
    """
    base = os.getenv("MLFLOW_UI_BASE")
    tracking = os.getenv("MLFLOW_TRACKING_URI")
    exp_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "tsmp-oop")
    if not base:
        return {"tracking_uri": tracking, "experiment": exp_name, "ui_url": None}

    ui_url = None
    try:
        import mlflow  # optional
        exp = mlflow.get_experiment_by_name(exp_name)
        if exp and getattr(exp, "experiment_id", None):
            from urllib.parse import quote
            q = quote(f"attributes.run_name = '{run_name}'", safe="")
            ui_url = f"{base}/#/experiments/{exp.experiment_id}/s?searchFilter={q}"
        else:
            ui_url = base
    except Exception:
        ui_url = base

    return {"tracking_uri": tracking, "experiment": exp_name, "ui_url": ui_url}

class RunRow(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    mlflow: dict | None = None
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
             to_char(r.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
             to_char(r.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
             COALESCE((r.config #>> '{config,horizon}')::int, NULL) AS horizon,
             (SELECT count(*) FROM predictions p WHERE p.run_id=r.run_id) AS n_predictions
      FROM runs r
      ORDER BY created_at DESC
      LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql)).mappings().first()
        if not row: raise HTTPException(404, "no runs")
        d = dict(row)
        d['mlflow'] = _mlflow_hint(d['run_id'])
        return d





@app.get("/predictions")
def predictions(
    run_id: str = Query(...),
    unique_id: list[str] | None = Query(None),
    start: str | None = Query(None, description="ISO8601 UTC, e.g. 2025-01-01T00:00:00Z"),
    end: str | None = Query(None, description="ISO8601 UTC (exclusive)"),
    order: str = Query("asc", description="asc|desc"),
    limit: int = 1000,
    offset: int = 0
):
    order_sql = "DESC" if str(order).lower() == "desc" else "ASC"\n    # --- multi-unique_id expansion ---\n    uids = []\n    if unique_id:\n        for u in (unique_id if isinstance(unique_id, list) else [unique_id]):\n            uids.extend([x.strip() for x in str(u).split(',') if x.strip()])\n    uids = list(dict.fromkeys(uids))  # de-dup\n    # 方言別 ds のISO8601Z整形
    dialect = engine.url.get_backend_name()\n    if dialect == 'postgresql':\n        ds_expr = "to_char(ds AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS ds"\n    else:\n        ds_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', ds) AS ds"\n
    dialect = engine.url.get_backend_name()
    # 方言別の式
    if dialect == 'postgresql':
        created_expr = "to_char(r.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at"
        updated_expr = "to_char(r.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at"
    else:
        # sqlite: TEXT型DATETIME想定
        created_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', r.created_at) AS created_at"
        updated_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', r.updated_at) AS updated_at"
    horizon_expr = "COALESCE((r.config #>> '{config,horizon}')::int, NULL) AS horizon" if dialect=='postgresql' else "CAST(json_extract(r.config,'$.config.horizon') AS INT) AS horizon"
    n_pred_expr = "(SELECT count(*) FROM predictions p WHERE p.run_id=r.run_id) AS n_predictions"
    _RUNS_WINDOW_COLSQL = {
        'run_id': 'r.run_id',
        'alias': 'r.alias',
        'model_name': 'r.model_name',
        'dataset': 'r.dataset',
        'status': 'r.status',
        'duration_sec': 'r.duration_sec',
        'created_at': created_expr,
        'updated_at': updated_expr,
        'horizon': horizon_expr,
        'n_predictions': n_pred_expr,
    }
    allowed = list(_RUNS_WINDOW_COLSQL.keys())
    # fields パース（未指定なら全列）。常に run_id は含める。
    if fields:
        req = [x.strip() for x in fields.split(',') if x.strip() in allowed]
        if 'run_id' not in req: req = ['run_id'] + req
        if not req: req = allowed
    else:
        req = allowed
    select_list = ', '.join(_RUNS_WINDOW_COLSQL[k] + f" AS {k}" if ' AS ' not in _RUNS_WINDOW_COLSQL[k] else _RUNS_WINDOW_COLSQL[k] for k in req)
    # ↑ horizon/created_at/updated_at/n_predictions は式に AS が含まれるのでそのまま

    start_dt = _parse_utc_ceil(start)
    end_dt = _parse_utc_ceil(end)

    params: dict = {"rid": run_id, "uid": unique_id, "lim": limit, "off": offset}
    where = ["run_id = :rid", "(:uid IS NULL OR unique_id = :uid)"]

    if dialect == "postgresql":
        if start_dt is not None: where.append("ds >= CAST(:pstart AS timestamptz)")
        if end_dt   is not None: where.append("ds <  CAST(:pend   AS timestamptz)")
        ts_select = 'to_char(ds AT TIME ZONE \'UTC\',\'YYYY-MM-DD"T"HH24:MI:SS"Z"\') AS ds'
        pstart, pend = start_dt, end_dt
    else:
        if start_dt is not None:
            params["pstart"] = start_dt.strftime("%Y-%m-%d %H:%M:%S"); where.append("ds >= :pstart")
        if end_dt is not None:
            params["pend"] = end_dt.strftime("%Y-%m-%d %H:%M:%S");   where.append("ds <  :pend")
        ts_select = "replace(ds,' ','T')||'Z' AS ds"
        pstart, pend = params.get("pstart"), params.get("pend")

    where_sql = " AND ".join(where)
    sql = f"""
      SELECT unique_id, {ts_select}, y_hat
      FROM predictions
      WHERE {where_sql}
      ORDER BY ds {order_sql}
      LIMIT :lim OFFSET :off
    """
    with engine.begin() as conn:
        base = {**params, "pstart": pstart, "pend": pend}
        rows = conn.execute(text(sql), base).mappings().all()
        return {"run_id": run_id, "count": len(rows), "items": [dict(r) for r in rows]}

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
    payload = {
        "ok": overall,
        "checks": {
            "duration_p95_sec": p95,
            "threshold_ms": KPI_DURATION_P95_MS,
            "predictions_ratio": ratio,
            "ratio_threshold": KPI_PREDICTIONS_RATIO,
            "latest_run_id": latest_run
        }
    }
    payload["thresholds"] = {"duration_p95_ms": KPI_DURATION_P95_MS, "predictions_ratio": KPI_PREDICTIONS_RATIO}
    return JSONResponse(content=payload, status_code=status_code)


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
    payload = {
        "ok": overall,
        "checks": {
            "duration_p95_sec": p95,
            "threshold_ms": KPI_DURATION_P95_MS,
            "predictions_ratio": ratio,
            "ratio_threshold": KPI_PREDICTIONS_RATIO,
            "latest_run_id": latest_run
        }
    }
    payload["thresholds"] = {"duration_p95_ms": KPI_DURATION_P95_MS, "predictions_ratio": KPI_PREDICTIONS_RATIO}
    return JSONResponse(content=payload, status_code=status_code)


@app.get("/runs")
def list_runs(limit: int = Query(100, ge=1, le=1000),
              offset: int = Query(0, ge=0),
              status: str | None = Query(None)):
    sql_items = """
      SELECT r.run_id, r.alias, r.model_name, r.dataset, r.status, r.duration_sec,
             to_char(r.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
             to_char(r.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
             COALESCE((r.config #>> '{config,horizon}')::int, NULL) AS horizon,
             (SELECT count(*) FROM predictions p WHERE p.run_id=r.run_id) AS n_predictions
      FROM runs r
      WHERE (:status IS NULL OR r.status = :status)
      ORDER BY r.created_at DESC
      LIMIT :lim OFFSET :off
    """
    sql_cnt = """
      SELECT count(*) FROM runs r
      WHERE (:status IS NULL OR r.status = :status)
    """
    with engine.begin() as conn:
        items = [dict(m) for m in conn.execute(text(sql_items), {"status": status, "lim": limit, "off": offset}).mappings().all()]
        total = int(conn.execute(text(sql_cnt), {"status": status}).scalar_one())
    return {"total": total, "count": len(items), "items": items}

def _parse_utc_ceil(s: str|None):
    if not s: return None
    t = s.replace("Z","").replace("z","")
    try:
        dt = datetime.fromisoformat(t)
    except Exception:
        return None
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt




@app.get("/runs/window")
def runs_window(
    start: str | None = Query(None, description="ISO8601 UTC, e.g. 2025-01-01T00:00:00Z"),
    end: str | None = Query(None, description="ISO8601 UTC (exclusive)"),
    status: str | None = Query(None),
    model_name: str | None = Query(None),
    alias: str | None = Query(None),
    fields: str | None = Query(None, description="comma-separated: run_id,alias,model_name,dataset,status,duration_sec,created_at,updated_at,horizon,n_predictions"),
    limit: int = 50,
    offset: int = 0
):
    dialect = engine.url.get_backend_name()
    # 方言別の式
    if dialect == 'postgresql':
        created_expr = "to_char(r.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at"
        updated_expr = "to_char(r.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at"
    else:
        # sqlite: TEXT型DATETIME想定
        created_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', r.created_at) AS created_at"
        updated_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', r.updated_at) AS updated_at"
    horizon_expr = "COALESCE((r.config #>> '{config,horizon}')::int, NULL) AS horizon" if dialect=='postgresql' else "CAST(json_extract(r.config,'$.config.horizon') AS INT) AS horizon"
    n_pred_expr = "(SELECT count(*) FROM predictions p WHERE p.run_id=r.run_id) AS n_predictions"
    _RUNS_WINDOW_COLSQL = {
        'run_id': 'r.run_id',
        'alias': 'r.alias',
        'model_name': 'r.model_name',
        'dataset': 'r.dataset',
        'status': 'r.status',
        'duration_sec': 'r.duration_sec',
        'created_at': created_expr,
        'updated_at': updated_expr,
        'horizon': horizon_expr,
        'n_predictions': n_pred_expr,
    }
    allowed = list(_RUNS_WINDOW_COLSQL.keys())
    # fields パース（未指定なら全列）。常に run_id は含める。
    if fields:
        req = [x.strip() for x in fields.split(',') if x.strip() in allowed]
        if 'run_id' not in req: req = ['run_id'] + req
        if not req: req = allowed
    else:
        req = allowed
    select_list = ', '.join(_RUNS_WINDOW_COLSQL[k] + f" AS {k}" if ' AS ' not in _RUNS_WINDOW_COLSQL[k] else _RUNS_WINDOW_COLSQL[k] for k in req)
    # ↑ horizon/created_at/updated_at/n_predictions は式に AS が含まれるのでそのまま

    start_dt = _parse_utc_ceil(start)
    end_dt = _parse_utc_ceil(end)

    params: dict = {"lim": limit, "off": offset}
    where = ["1=1"]
    if status:
        where.append("r.status = :status"); params["status"] = status
    if model_name:
        where.append("r.model_name = :model_name"); params["model_name"] = model_name
    if alias:
        where.append("r.alias = :alias"); params["alias"] = alias

    if dialect == "postgresql":
        if start_dt is not None: where.append("r.created_at >= CAST(:start AS timestamptz)")
        if end_dt   is not None: where.append("r.created_at <  CAST(:end   AS timestamptz)")
        ts_cols = """to_char(r.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                     to_char(r.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at"""
        horizon_sql = "COALESCE((r.config #>> '{config,horizon}')::int, NULL) AS horizon"
        start_param, end_param = start_dt, end_dt
    else:
        # SQLite: 文字列比較 & ISO8601Z 生成
        if start_dt is not None:
            params["start"] = start_dt.strftime("%Y-%m-%d %H:%M:%S"); where.append("r.created_at >= :start")
        if end_dt is not None:
            params["end"] = end_dt.strftime("%Y-%m-%d %H:%M:%S");   where.append("r.created_at <  :end")
        ts_cols = "replace(r.created_at,' ','T')||'Z' AS created_at, replace(r.updated_at,' ','T')||'Z' AS updated_at"
        horizon_sql = "NULL AS horizon"
        start_param, end_param = params.get("start"), params.get("end")

    where_sql = " AND ".join(where)
    sql_total = f"SELECT count(*) FROM runs r WHERE {where_sql}"
    sql = f"""
      SELECT r.run_id, r.alias, r.model_name, r.dataset, r.status, r.duration_sec,
             {ts_cols},
             {horizon_sql},
             (SELECT count(*) FROM predictions p WHERE p.run_id = r.run_id) AS n_predictions
      FROM runs r
      WHERE {where_sql}
      ORDER BY r.created_at DESC
      LIMIT :lim OFFSET :off
    """
    with engine.begin() as conn:
        base = {**params, "start": start_param, "end": end_param}
        total = int(conn.execute(text(sql_total), base).scalar_one())
        items = [dict(row) for row in conn.execute(text(sql), base).mappings().all()]
        return {"total": total, "count": len(items), "items": items}
