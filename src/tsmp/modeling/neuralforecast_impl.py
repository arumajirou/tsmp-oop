import pandas as pd
from typing import Dict, Any
from .base import AbstractModel

class BaselineMean(AbstractModel):
    """Simple baseline model to keep package runnable offline."""
    def fit(self, df: pd.DataFrame) -> None:
        self._df = df.copy()
        self._means = self._df.groupby("unique_id")["y"].mean()

    def predict(self, horizon: int) -> pd.DataFrame:
        if not hasattr(self, "_means"):
            raise RuntimeError("Model not fitted")
        frames = []
        for uid, mean in self._means.items():
            last_ds = self._df[self._df["unique_id"]==uid]["ds"].max()
            for i in range(1, horizon+1):
                frames.append({"unique_id": uid, "ds": last_ds + pd.Timedelta(days=i), "y_hat": float(mean)})
        return pd.DataFrame(frames)
