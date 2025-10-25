import argparse
from tsmp.orchestration.pipeline import Pipeline

def main():
    p = argparse.ArgumentParser(description="TSMP-OOP orchestrator")
    p.add_argument("command", choices=["run"])
    p.add_argument("--run-config", default="configs/run_spec.yaml")
    p.add_argument("--fe-config", default="configs/fe_config.yaml")
    p.add_argument("--constraints", default="configs/constraints.yaml")
    p.add_argument("--capabilities", default="configs/model_capability.yaml")
    args = p.parse_args()

    if args.command == "run":
        pl = Pipeline(model_capability_path=args.capabilities)
        out = pl.run(args.run_config, args.fe_config, args.constraints)
        print(out)

if __name__ == "__main__":
    main()
