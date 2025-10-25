import uuid, time
from dataclasses import dataclass
from typing import Dict, Any, Type
import pandas as pd

from .base import AbstractModel
from .neuralforecast_impl import BaselineMean

MODEL_REGISTRY: Dict[str, Type[AbstractModel]] = {
    "BaselineMean": BaselineMean,
}

@dataclass
class RunResult:
    run_id: str
    duration_sec: float
    predictions: pd.DataFrame

class TrainingSession:
    def __init__(self, model_name: str, hyperparams: Dict[str, Any]) -> None:
        if model_name not in MODEL_REGISTRY:
            raise ValueError(f"model not registered: {model_name}")
        self.model = MODEL_REGISTRY[model_name](hyperparams)

    def run(self, df: pd.DataFrame, horizon: int) -> RunResult:
        run_id = uuid.uuid4().hex[:12]
        t0 = time.time()
        self.model.fit(df)
        preds = self.model.predict(horizon)
        return RunResult(run_id=run_id, duration_sec=time.time()-t0, predictions=preds)
