import os, mlflow
from typing import Dict, Any

class Tracker:
  def __init__(self, tracking_uri: str | None = None) -> None:
    self._uri = tracking_uri or os.getenv("MLFLOW_TRACKING_URI", "file:mlruns")

  def start(self, run_name: str) -> str:
    mlflow.set_tracking_uri(self._uri)
    run = mlflow.start_run(run_name=run_name)
    return run.info.run_id

  def log_params(self, params: Dict[str, Any]) -> None:
    mlflow.log_params(params)

  def log_metrics(self, metrics: Dict[str, float]) -> None:
    mlflow.log_metrics(metrics)

  def end(self) -> None:
    mlflow.end_run()
