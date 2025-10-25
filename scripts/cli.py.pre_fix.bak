#!/usr/bin/env python
import argparse, json, os
from pathlib import Path

from tsmp.orchestration.pipeline import Pipeline

def run_once(run_config, fe_config, constraints):
    pipe = Pipeline(run_config_path=run_config,
                    feature_config_path=fe_config,
                    constraints_path=constraints)
    result = pipe.run()
    return result

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    spr = sub.add_parser("run")
    spr.add_argument("--run-config", default="configs/run_spec.yaml")
    spr.add_argument("--fe-config", default="configs/fe_config.yaml")
    spr.add_argument("--constraints", default="configs/constraints.yaml")
    spr.add_argument("--capabilities", default=None)
    spr.add_argument("--with-mlflow", action="store_true", help="log to MLflow if MLFLOW_TRACKING_URI is configured")

    args = ap.parse_args()

    if args.cmd == "run":
        result = run_once(args.run_config, args.fe_config, args.constraints)

        # 成果物: JSON を outputs/last_run.json に保存（機械可読）
        Path("outputs").mkdir(parents=True, exist_ok=True)
        out_path = Path("outputs/last_run.json")
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

        # 必要に応じて MLflow へも記録
        if args.with-mlflow:
            try:
                import mlflow
                mlflow.set_experiment("tsmp-oop")
                with mlflow.start_run(run_name=str(result.get("run_id"))):
                    mlflow.log_dict(result, "result.json")
                    if isinstance(result, dict):
                        # 代表的な数値メトリクスを抽出
                        if "duration_sec" in result:
                            mlflow.log_metric("duration_sec", float(result["duration_sec"]))
                        if "predictions_rows" in result:
                            mlflow.log_metric("predictions_rows", float(result["predictions_rows"]))
                        if "dq" in result and isinstance(result["dq"], dict):
                            for k, v in result["dq"].items():
                                if isinstance(v, (int, float)):
                                    mlflow.log_metric(f"dq_{k}", float(v))
                    # タグ少々
                    mlflow.set_tags({"phase": "P1", "component": "cli"})
            except Exception as e:
                # 失敗しても処理自体は続行（標準出力に警告）
                print(f"[warn] MLflow logging skipped: {e}")

        # 標準出力にも JSON を出す（パイプで拾える）
        print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()
