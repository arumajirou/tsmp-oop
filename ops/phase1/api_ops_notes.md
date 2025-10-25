# Health & SLO
- KPI_DURATION_P95_MS (default 5000ms), KPI_PREDICTIONS_RATIO (default 1.0)
- /health returns 200 or 503 with JSON:
  { ok: bool, checks: { duration_p95_sec, threshold_ms, predictions_ratio, ratio_threshold, latest_run_id } }
- LB/Ingress: path=/health, 503扱いで切替
- Alert: 連続N回503でアラート（閾値は監視側で設定）
# API
- GET /runs?limit,offset,status
- GET /runs/latest  … mlflow {tracking_uri, experiment, ui_url}
- GET /predictions?run_id[&unique_id]&limit&offset … ds=ISO8601(UTC)
