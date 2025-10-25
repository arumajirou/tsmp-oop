from typing import Dict, Any
import pandas as pd

class DataQualityService:
    def profile(self, df: pd.DataFrame) -> Dict[str, Any]:
        n_rows = len(df)
        missing_rate = float(df.isna().mean().mean()) if n_rows else 0.0
        return {
            "n_rows": n_rows,
            "missing_rate": missing_rate,
        }
