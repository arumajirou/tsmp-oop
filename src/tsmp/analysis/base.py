import pandas as pd
from typing import Dict, Any

class Analyzer:
    def analyze(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {"rows": len(df)}
