from typing import Dict, Any

class ContextAdapter:
    """Adapts configuration to runtime context (e.g., hardware, dataset size)."""
    def adapt(self, run_cfg: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        rc = dict(run_cfg)
        if context.get("memory_mb", 0) < 2048:
            # shrink window under tight memory
            hp = rc.setdefault("hyperparams", {})
            if "window" in hp:
                hp["window"] = max(3, min(hp["window"], 7))
        return rc
