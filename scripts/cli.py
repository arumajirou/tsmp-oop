#!/usr/bin/env python
import argparse, json, os
from pathlib import Path

from tsmp.orchestration.pipeline import Pipeline

def run_once(run_config, fe_config, constraints, capabilities_path):
    """
    新旧 Pipeline API 互換:
      新: Pipeline(run_config_path=..., feature_config_path=..., constraints_path=..., model_capability_path=...) -> .run()
      旧: Pipeline(model_capability_path=...) -> .run(run_config, fe_config, constraints)
    """
    # 可能なら capabilities を渡す
    cap_path = capabilities_path if capabilities_path else None
    # 新APIトライ
    try:
        pipe = Pipeline(run_config_path=run_config,
                        feature_config_path=fe_config,
                        constraints_path=constraints,
                        model_capability_path=cap_path)
        return pipe.run()
    except TypeError:
        # 旧APIフォールバック
        pipe = Pipeline(model_capability_path=cap_path) if cap_path else Pipeline()
        return pipe.run(run_config, fe_config, constraints)

def main():
    ap = argparse.ArgumentParser(description="TSMP-OOP orchestrator")
    sub = ap.add_subparsers(dest="cmd", required=True)

    spr = sub.add_parser("run")
    spr.add_argument("--run-config", default="configs/run_spec.yaml")
    spr.add_argument("--fe-config", default="configs/fe_config.yaml")
    spr.add_argument("--constraints", default="configs/constraints.yaml")
    # 旧デフォルトを復活して下位互換性を確保
    spr.add_argument("--capabilities", default="configs/model_capability.yaml")
    spr.add_argument("--with-mlflow", action="store_true",
                     help="log to MLflow if MLFLOW_TRACKING_URI is configured")

    args = ap.parse_args()

    if args.cmd == "run":
        result = run_once(args.run_config, args.fe_config, args.constraints, args.capabilities)

        # 成果物: JSON を outputs/last_run.json に保存（機械可読）
        Path("outputs").mkdir(parents=True, exist_ok=True)
        out_path = Path("outputs/last_run.json")
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

        # 必要に応じて MLflow へ記録
        if args.with_mlflow:
            try:
                import mlflow
                mlflow.set_experiment("tsmp-oop")
                with mlflow.start_run(run_name=str(result.get("run_id"))):
                    mlflow.log_dict(result, "result.json")
                    if isinstance(result, dict):
                        if "duration_sec" in result: mlflow.log_metric("duration_sec", float(result["duration_sec"]))
                        if "predictions_rows" in result: mlflow.log_metric("predictions_rows", float(result["predictions_rows"]))
                        if "dq" in result and isinstance(result["dq"], dict):
                            for k, v in result["dq"].items():
                                if isinstance(v, (int, float)):
                                    mlflow.log_metric(f"dq_{k}", float(v))
                    mlflow.set_tags({"phase": "P1", "component": "cli"})
            except Exception as e:
                print(f"[warn] MLflow logging skipped: {e}")

        # 標準出力にも JSON
        print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()
