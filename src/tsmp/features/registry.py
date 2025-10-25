from typing import Dict, Any, List
from .base import Feature, LagFeature, DayOfWeekFeature

def build_features(spec: List[Dict[str, Any]]) -> List[Feature]:
    features: List[Feature] = []
    for item in spec:
        t = item["type"]
        params = item.get("params", {})
        if t == "lag":
            features.append(LagFeature(**params))
        elif t == "day_of_week":
            features.append(DayOfWeekFeature(**params))
        else:
            raise ValueError(f"Unknown feature type: {t}")
    return features

def run_features(df, features: List[Feature]):
    out = df.copy()
    for f in features:
        out = f.apply(out)
    return out
