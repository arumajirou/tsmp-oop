from typing import Dict, Any
from ..core.config import ConstraintSpec

class NonLocalConstraintSolver:
    """Apply pipeline-wide constraints to configs and fe specs."""
    def apply(self, run_cfg: Dict[str, Any], fe_cfg: Dict[str, Any], spec: ConstraintSpec) -> tuple[Dict[str, Any], Dict[str, Any]]:
        rc = dict(run_cfg)
        fc = dict(fe_cfg)
        horizon = rc.get("horizon", 1)

        # simple estimation of feature cost
        est_features = "high" if len(fc.get("features", [])) > 1 else "low"

        for rule in spec.rules:
            when = rule.when
            then = rule.then
            if "horizon_max" in when and horizon <= when["horizon_max"]:
                wm = then.get("cap_params", {}).get("window_max")
                if wm is not None:
                    hp = rc.setdefault("hyperparams", {})
                    if "window" in hp and hp["window"] > wm:
                        hp["window"] = wm
            if when.get("estimated_features") == est_features and "reduce_features_by" in then:
                factor = then["reduce_features_by"]
                # drop a proportion of trailing features to respect memory guardrail
                feats = fc.get("features", [])
                keep = max(1, int(len(feats)*(1.0-factor)))
                fc["features"] = feats[:keep]
        return rc, fc
