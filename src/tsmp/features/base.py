from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any

class Feature(ABC):
    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame: ...

class LagFeature(Feature):
    def __init__(self, column: str, lags: list[int]) -> None:
        self.column = column
        self.lags = lags

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for k in self.lags:
            out[f"{self.column}_lag_{k}"] = out.groupby("unique_id")[self.column].shift(k)
        return out

class DayOfWeekFeature(Feature):
    def __init__(self, column: str) -> None:
        self.column = column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["day_of_week"] = out[self.column].dt.dayofweek
        return out
