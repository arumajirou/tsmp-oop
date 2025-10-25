import yaml, pandas as pd
from typing import Dict, Any
from ..core.config import RunConfig, FeatureConfig, ConstraintSpec
from ..core.logging import setup_logging
from ..data.repository import ParquetRepository
from ..data.quality import DataQualityService
from ..features.registry import build_features, run_features
from ..modeling.runner import TrainingSession
from ..modeling.capability import CapabilityValidator
from ..orchestration.constraints import NonLocalConstraintSolver
from ..orchestration.context_adapter import ContextAdapter
from ..monitoring.resource import ResourceMonitor

class Pipeline:
    def __init__(self, model_capability_path: str) -> None:
        self._cap = CapabilityValidator(model_capability_path)
        self._solver = NonLocalConstraintSolver()
        self._adapter = ContextAdapter()
        self._repo = ParquetRepository()
        self._dq = DataQualityService()
        self._rm = ResourceMonitor()

    def run(self, run_config_path: str, fe_config_path: str, constraints_path: str) -> Dict[str, Any]:
        setup_logging("INFO")
        run_cfg = RunConfig(**yaml.safe_load(open(run_config_path, 'r', encoding='utf-8'))).model_dump()
        fe_cfg = FeatureConfig(**yaml.safe_load(open(fe_config_path, 'r', encoding='utf-8'))).model_dump()
        conspec = ConstraintSpec(**yaml.safe_load(open(constraints_path, 'r', encoding='utf-8')))

        # Non-local constraint optimization
        run_cfg, fe_cfg = self._solver.apply(run_cfg, fe_cfg, conspec)

        # Context adaptation
        ctx = self._rm.snapshot()
        run_cfg = self._adapter.adapt(run_cfg, ctx)

        ok, errs = self._cap.validate(run_cfg["model_name"], run_cfg.get("hyperparams", {}))
        if not ok:
            raise ValueError(f"capability validation failed: {errs}")

        df = self._repo.load_observations(run_cfg["dataset_path"])
        if df.empty:
            # For demo: fabricate tiny dataset if none present
            import numpy as np, pandas as pd
            idx = pd.date_range("2024-01-01", periods=30, freq="D")
            df = pd.DataFrame({"unique_id": ["A"]*30, "ds": idx, "y": np.random.rand(30)})

        dq = self._dq.profile(df)

        feats = build_features(fe_cfg.get("features", []))
        df_f = run_features(df, feats).dropna().reset_index(drop=True)

        session = TrainingSession(run_cfg["model_name"], run_cfg.get("hyperparams", {}))
        result = session.run(df_f, run_cfg["horizon"])

        return {
            "run_id": result.run_id,
            "duration_sec": result.duration_sec,
            "dq": dq,
            "predictions_rows": len(result.predictions),
            "context": ctx
        }
