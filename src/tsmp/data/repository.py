from abc import ABC, abstractmethod
from typing import Iterable
import pandas as pd
from sqlalchemy import create_engine, text

class AbstractRepository(ABC):
    @abstractmethod
    def load_observations(self, dataset_path: str) -> pd.DataFrame: ...

    @abstractmethod
    def save_predictions(self, run_id: str, df: pd.DataFrame) -> None: ...

class ParquetRepository(AbstractRepository):
    def load_observations(self, dataset_path: str) -> pd.DataFrame:
        return pd.read_parquet(dataset_path)

    def save_predictions(self, run_id: str, df: pd.DataFrame) -> None:
        # no-op for local parquet demo
        pass

class SQLRepository(AbstractRepository):
    def __init__(self, dsn: str) -> None:
        self._engine = create_engine(dsn, pool_pre_ping=True)

    def load_observations(self, dataset_path: str) -> pd.DataFrame:
        # In a real system, dataset_path could key into a DB table/view
        with self._engine.begin() as conn:
            # Demo: read from a staging table if present, else empty
            try:
                return pd.read_sql("SELECT unique_id, ds, y FROM observations", conn, parse_dates=["ds"])
            except Exception:
                import pandas as pd
                return pd.DataFrame(columns=["unique_id","ds","y"])

    def save_predictions(self, run_id: str, df: pd.DataFrame) -> None:
        with self._engine.begin() as conn:
            df = df.copy()
            df["run_id"] = run_id
            df.to_sql("predictions", conn, if_exists="append", index=False)
