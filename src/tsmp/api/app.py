# src/tsmp/api/app.py
from fastapi import FastAPI, HTTPException, Query, status as http_status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import os

# --- Prometheus ---
from prometheus_client import Counter, Histogram, Gauge, CONTENT_TYPE_LATEST, generate_latest

app = FastAPI(title="tsmp-oop")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DSN = os.getenv("POSTGRES_DSN", "postgresql:///tsmodeling")
engine = create_engine(DSN, pool_pre_ping=True)

# ---------- Prometheus metrics ----------
REQ_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)

REQ_LATENCY = Histogram(
    "http_request_duration_seconds",
    "Request latency (seconds)",
    ["method", "path", "status"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
)

TSMP_HEALTH_OK = Gauge("tsmp_health_ok", "1 if /health is OK else 0")
TSMP_RUNS_COUNT = Gauge("tsmp_runs_count", "Total number of rows in runs table")
TSMP_PREDICTIONS_COUNT = Gauge("tsmp_predictions_count", "Total number of predictions for latest run_id")

def _safe_path_label(request: Request) -> str:
    # ルートテンプレートを優先（クエリは含めない）
    route = request.scope.get("route")
    if route and getattr(route, "path", None):
        return route.path
    return request.url.path

@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    method = request.method
    path = _safe_path_label(request)
    start = datetime.now().timestamp()
    try:
        response = await call_next(request)
        status = str(response.status_code)
        return response
    except Exception:
        # 例外は 500 としてカウント
        status = "500"
        REQ_COUNT.labels(method=method, path=path, status=status).inc()
        dur = max(0.0, datetime.now().timestamp() - start)
        REQ_LATENCY.labels(method=method, path=path, status=status).observe(dur)
        raise
    finally:
        # 正常終了/異常終了どちらもここでレコード
        end = datetime.now().timestamp()
        dur = max(0.0, end - start)
        # status が未設定（例外ルート）の場合は上の except で処理済み
        if "status" not in locals() or status == "500":
            pass
        else:
            REQ_COUNT.labels(method=method, path=path, status=status).inc()
            REQ_LATENCY.labels(method=method, path=path, status=status).observe(dur)

@app.get("/metrics")
def metrics():
    # その時点のメトリクスを出力
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# ---------- helpers ----------
def _mlflow_hint(run_name: str) -> dict | None:
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

def _parse_utc_ceil(s: str | None):
    if not s:
        return None
    t = s.replace("Z", "").replace("z", "")
    try:
        dt = datetime.fromisoformat(t)
    except Exception:
        return None
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

def _list_table_columns(conn, table: str, dialect: str) -> list[str]:
    if dialect == "sqlite":
        # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        return [row[1] for row in conn.execute(text(f"PRAGMA table_info({table})")).all()]
    else:
        rows = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = :t"
            ),
            {"t": table},
        ).all()
        return [r[0] for r in rows]

def _resolve_pred_y_expr(conn, dialect: str) -> str:
    cols = set(_list_table_columns(conn, "predictions", dialect))
    for cand in ("yhat", "y_pred", "prediction", "y", "value", "y_hat", "yhat_mean", "pred", "forecast"):
        if cand in cols:
            return f"p.{cand} AS yhat"
    return "NULL AS yhat"

def _view_exists(conn, name: str, dialect: str) -> bool:
    if dialect == "sqlite":
        q = "SELECT name FROM sqlite_master WHERE type='view' AND name=:n"
        return conn.execute(text(q), {"n": name}).first() is not None
    else:
        q = """
        SELECT 1
          FROM information_schema.views
         WHERE table_schema = current_schema() AND table_name = :n
        """
        return conn.execute(text(q), {"n": name}).first() is not None

def _update_domain_gauges(conn, dialect: str):
    # runs 総数
    total_runs = int(conn.execute(text("SELECT count(*) FROM runs")).scalar_one())
    TSMP_RUNS_COUNT.set(total_runs)
    # 最新 run の predictions 件数
    latest = conn.execute(text("SELECT run_id FROM runs ORDER BY created_at DESC LIMIT 1")).first()
    if latest:
        rid = latest[0]
        n_pred = int(conn.execute(text("SELECT count(*) FROM predictions WHERE run_id=:r"), {"r": rid}).scalar_one())
        TSMP_PREDICTIONS_COUNT.set(n_pred)
    else:
        TSMP_PREDICTIONS_COUNT.set(0)

# ---------- models ----------
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
    n_predictions: int | None = None

# ---------- /runs (list) ----------
@app.get("/runs")
def list_runs(
    status_filter: str | None = Query(None, alias="status"),
    limit: int = 50,
    offset: int = 0,
):
    dialect = engine.url.get_backend_name()
    if dialect == "postgresql":
        created_expr = "to_char(r.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at"
        updated_expr = "to_char(r.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at"
        horizon_expr = "COALESCE((r.config #>> '{config,horizon}')::int, NULL) AS horizon"
    else:
        created_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', r.created_at) AS created_at"
        updated_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', r.updated_at) AS updated_at"
        horizon_expr = "CAST(json_extract(r.config,'$.config.horizon') AS INT) AS horizon"

    n_pred_expr = "(SELECT count(*) FROM predictions p WHERE p.run_id=r.run_id) AS n_predictions"

    where_sql = "(:status IS NULL OR r.status = :status)"
    params = {"status": status_filter, "lim": limit, "off": offset}

    sql_items = f"""
      SELECT r.run_id, r.alias, r.model_name, r.dataset, r.status, r.duration_sec,
             {created_expr}, {updated_expr}, {horizon_expr}, {n_pred_expr}
      FROM runs r
      WHERE {where_sql}
      ORDER BY r.created_at DESC
      LIMIT :lim OFFSET :off
    """
    sql_cnt = f"SELECT count(*) FROM runs r WHERE {where_sql}"

    with engine.begin() as conn:
        items = [dict(m) for m in conn.execute(text(sql_items), params).mappings().all()]
        total = int(conn.execute(text(sql_cnt), params).scalar_one())
        # ゲージ更新（副作用OK）
        _update_domain_gauges(conn, dialect)

    return {"total": total, "count": len(items), "items": items}

# ---------- /runs/latest ----------
@app.get("/runs/latest", response_model=RunRow)
def latest_run():
    dialect = engine.url.get_backend_name()
    if dialect == "postgresql":
        created_expr = "to_char(r.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at"
        updated_expr = "to_char(r.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at"
        horizon_expr = "COALESCE((r.config #>> '{config,horizon}')::int, NULL) AS horizon"
    else:
        created_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', r.created_at) AS created_at"
        updated_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', r.updated_at) AS updated_at"
        horizon_expr = "CAST(json_extract(r.config,'$.config.horizon') AS INT) AS horizon"

    n_pred_expr = "(SELECT count(*) FROM predictions p WHERE p.run_id=r.run_id) AS n_predictions"

    sql = f"""
      SELECT r.run_id, r.alias, r.model_name, r.dataset, r.status, r.duration_sec,
             {created_expr}, {updated_expr}, {horizon_expr}, {n_pred_expr}
      FROM runs r
      ORDER BY r.created_at DESC
      LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql)).mappings().first()
        if not row:
            raise HTTPException(404, "no runs")
        d = dict(row)
        d["mlflow"] = _mlflow_hint(d["run_id"])
        # ゲージ更新
        _update_domain_gauges(conn, dialect)
        return d

# ---------- /predictions ----------
@app.get("/predictions")
def predictions(
    run_id: str = Query(...),
    unique_id: list[str] | None = Query(None),
    start: str | None = Query(None, description="ISO8601 UTC, e.g. 2025-01-01T00:00:00Z"),
    end: str | None = Query(None, description="ISO8601 UTC (exclusive)"),
    order: str = Query("asc", description="asc|desc"),
    limit: int = 1000,
    offset: int = 0,
):
    # unique_id は配列/カンマ両対応
    uids: list[str] = []
    if unique_id:
        for u in unique_id:
            uids.extend([x.strip() for x in str(u).split(",") if x.strip()])
    uids = list(dict.fromkeys(uids))

    order_sql = "DESC" if str(order).lower() == "desc" else "ASC"

    dialect = engine.url.get_backend_name()
    start_dt = _parse_utc_ceil(start)
    end_dt = _parse_utc_ceil(end)

    where = ["p.run_id = :rid"]
    params: dict = {"rid": run_id, "lim": limit, "off": offset}

    if dialect == "postgresql":
        ds_expr = "to_char(p.ds AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS ds"
        if start_dt is not None:
            where.append("p.ds >= CAST(:start AS timestamptz)")
            params["start"] = start_dt
        if end_dt is not None:
            where.append("p.ds < CAST(:end AS timestamptz)")
            params["end"] = end_dt
    else:
        ds_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', p.ds) AS ds"
        if start_dt is not None:
            params["start"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")
            where.append("p.ds >= :start")
        if end_dt is not None:
            params["end"] = end_dt.strftime("%Y-%m-%d %H:%M:%S")
            where.append("p.ds < :end")

    # IN 句（ユニークID）
    if uids:
        ph = []
        for i, u in enumerate(uids):
            k = f"u{i}"
            params[k] = u
            ph.append(":" + k)
        where.append(f"p.unique_id IN ({','.join(ph)})")

    where_sql = " AND ".join(where)

    with engine.begin() as conn:
        use_view = _view_exists(conn, "predictions_view", dialect)
        if use_view:
            y_expr = "p.yhat AS yhat"
            from_src = "predictions_view p"
        else:
            y_expr = _resolve_pred_y_expr(conn, dialect)
            from_src = "predictions p"

        sql = f"""
          SELECT p.run_id, p.unique_id, {ds_expr}, {y_expr}
          FROM {from_src}
          WHERE {where_sql}
          ORDER BY p.ds {order_sql}
          LIMIT :lim OFFSET :off
        """
        items = [dict(m) for m in conn.execute(text(sql), params).mappings().all()]
        # ゲージ更新
        _update_domain_gauges(conn, dialect)

    return {"total": None, "count": len(items), "items": items}

# ---------- /runs/window ----------
@app.get("/runs/window")
def runs_window(
    start: str | None = Query(None, description="ISO8601 UTC, e.g. 2025-01-01T00:00:00Z"),
    end: str | None = Query(None, description="ISO8601 UTC (exclusive)"),
    status_filter: str | None = Query(None, alias="status"),
    model_name: str | None = Query(None),
    alias: str | None = Query(None),
    fields: str | None = Query(
        None,
        description="comma-separated: run_id,alias,model_name,dataset,status,duration_sec,created_at,updated_at,horizon,n_predictions",
    ),
    limit: int = 50,
    offset: int = 0,
):
    dialect = engine.url.get_backend_name()
    if dialect == "postgresql":
        created_expr = "to_char(r.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at"
        updated_expr = "to_char(r.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at"
        horizon_expr = "COALESCE((r.config #>> '{config,horizon}')::int, NULL) AS horizon"
    else:
        created_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', r.created_at) AS created_at"
        updated_expr = "strftime('%Y-%m-%dT%H:%M:%SZ', r.updated_at) AS updated_at"
        horizon_expr = "CAST(json_extract(r.config,'$.config.horizon') AS INT) AS horizon"

    n_pred_expr = "(SELECT count(*) FROM predictions p WHERE p.run_id=r.run_id) AS n_predictions"

    colsql = {
        "run_id": "r.run_id",
        "alias": "r.alias",
        "model_name": "r.model_name",
        "dataset": "r.dataset",
        "status": "r.status",
        "duration_sec": "r.duration_sec",
        "created_at": created_expr,
        "updated_at": updated_expr,
        "horizon": horizon_expr,
        "n_predictions": n_pred_expr,
    }
    allowed = list(colsql.keys())

    if fields:
        req = [x.strip() for x in fields.split(",") if x.strip() in allowed]
        if "run_id" not in req:
            req = ["run_id"] + req
        if not req:
            req = allowed
    else:
        req = allowed

    def _alias_if_needed(expr: str, name: str) -> str:
        return expr if " AS " in expr.upper() else f"{expr} AS {name}"

    select_list = ", ".join(_alias_if_needed(colsql[k], k) for k in req)

    start_dt = _parse_utc_ceil(start)
    end_dt = _parse_utc_ceil(end)

    params: dict = {"lim": limit, "off": offset}
    where = ["1=1"]
    if status_filter:
        where.append("r.status = :status")
        params["status"] = status_filter
    if model_name:
        where.append("r.model_name = :model_name")
        params["model_name"] = model_name
    if alias:
        where.append("r.alias = :alias")
        params["alias"] = alias

    if dialect == "postgresql":
        if start_dt is not None:
            where.append("r.created_at >= CAST(:start AS timestamptz)")
            params["start"] = start_dt
        if end_dt is not None:
            where.append("r.created_at < CAST(:end AS timestamptz)")
            params["end"] = end_dt
    else:
        if start_dt is not None:
            params["start"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")
            where.append("r.created_at >= :start")
        if end_dt is not None:
            params["end"] = end_dt.strftime("%Y-%m-%d %H:%M:%S")
            where.append("r.created_at < :end")

    where_sql = " AND ".join(where)
    sql_total = f"SELECT count(*) FROM runs r WHERE {where_sql}"
    sql = f"""
      SELECT {select_list}
      FROM runs r
      WHERE {where_sql}
      ORDER BY r.created_at DESC
      LIMIT :lim OFFSET :off
    """

    with engine.begin() as conn:
        total = int(conn.execute(text(sql_total), params).scalar_one())
        items = [dict(row) for row in conn.execute(text(sql), params).mappings().all()]
        # ゲージ更新
        _update_domain_gauges(conn, dialect)
        return {"total": total, "count": len(items), "items": items}

# ---------- /health ----------
@app.get("/health")
def health():
    KPI_DURATION_P95_MS = int(os.getenv("KPI_DURATION_P95_MS", "5000"))
    KPI_PREDICTIONS_RATIO = float(os.getenv("KPI_PREDICTIONS_RATIO", "1.0"))
    dialect = engine.url.get_backend_name()

    if dialect == "postgresql":
        sql_p95 = "SELECT COALESCE(percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_sec), 0) AS p95 FROM runs"
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
    else:
        sql_p95 = None
        sql_ratio = """
          WITH latest AS (
            SELECT run_id, CAST(json_extract(config,'$.config.horizon') AS INT) AS horizon
            FROM runs ORDER BY created_at DESC LIMIT 1
          ),
          s AS (SELECT count(DISTINCT p.unique_id) AS n_series
                FROM predictions p JOIN latest l ON p.run_id = l.run_id),
          c AS (SELECT count(*) AS n_pred
                FROM predictions p JOIN latest l ON p.run_id = l.run_id)
          SELECT l.run_id, l.horizon, s.n_series, c.n_pred,
                 CASE WHEN s.n_series>0 AND l.horizon>0
                      THEN CAST(c.n_pred AS FLOAT) / (s.n_series * l.horizon)
                      ELSE NULL END AS ratio
          FROM latest l CROSS JOIN s CROSS JOIN c
        """

    with engine.begin() as conn:
        if dialect == "postgresql":
            p95 = float(conn.execute(text(sql_p95)).scalar_one())
        else:
            rows = conn.execute(text("SELECT duration_sec FROM runs WHERE duration_sec IS NOT NULL ORDER BY duration_sec")).all()
            vals = [float(x[0]) for x in rows]
            if vals:
                # 線形補間
                k = (len(vals) - 1) * 0.95
                f = int(k)
                c = min(f + 1, len(vals) - 1)
                p95 = vals[f] if c == f else (vals[f] * (c - k) + vals[c] * (k - f))
            else:
                p95 = 0.0

        r = conn.execute(text(sql_ratio)).mappings().first()
        ratio = (float(r["ratio"]) if r and r["ratio"] is not None else None)
        latest_run = (r["run_id"] if r else None)
        # ゲージ更新
        _update_domain_gauges(conn, dialect)

    ok_p95 = (p95 * 1000.0) <= KPI_DURATION_P95_MS
    ok_ratio = (ratio is None) or (ratio >= KPI_PREDICTIONS_RATIO)
    overall = bool(ok_p95 and ok_ratio)
    # /health OK/NG をゲージに反映
    TSMP_HEALTH_OK.set(1 if overall else 0)

    status_code = http_status.HTTP_200_OK if overall else http_status.HTTP_503_SERVICE_UNAVAILABLE
    payload = {
        "ok": overall,
        "checks": {"duration_p95_sec": p95, "predictions_ratio": ratio, "latest_run_id": latest_run},
        "thresholds": {"duration_p95_ms": KPI_DURATION_P95_MS, "predictions_ratio": KPI_PREDICTIONS_RATIO},
    }
    return JSONResponse(content=payload, status_code=status_code)
