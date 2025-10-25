from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any

class AbstractModel(ABC):
    def __init__(self, hyperparams: Dict[str, Any]):
        self.hyperparams = hyperparams

    @abstractmethod
    def fit(self, df: pd.DataFrame) -> None: ...

    @abstractmethod
    def predict(self, horizon: int) -> pd.DataFrame: ...
