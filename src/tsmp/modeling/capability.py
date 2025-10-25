import yaml
from typing import Dict, Any, Tuple, List

class CapabilityValidator:
    def __init__(self, path: str) -> None:
        with open(path, 'r', encoding='utf-8') as f:
            self.spec = yaml.safe_load(f)

    def validate(self, model_name: str, hyperparams: Dict[str, Any]) -> Tuple[bool, List[str]]:
        models = self.spec.get("models", {})
        if model_name not in models:
            return False, [f"unknown model: {model_name}"]
        m = models[model_name]
        allowed = set(m.get("allowed_params", []))
        errs = []
        for k in hyperparams.keys():
            if k not in allowed:
                errs.append(f"param not allowed: {k}")
        constraints = m.get("constraints", {})
        for k, c in constraints.items():
            if "allowed" in c and k in hyperparams and hyperparams[k] not in c["allowed"]:
                errs.append(f"{k} not in allowed {c['allowed']}")
        return len(errs)==0, errs
