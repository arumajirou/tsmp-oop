# TSMP-OOP (Time Series Modeling Platform — OOP Edition)

Object-oriented, extensible platform for time-series ML with non-local constraint optimization
and context-adaptive orchestration. Modules interact via explicit interfaces and a small
dependency-injection container.

## Quick start
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
python scripts/cli.py --help
python scripts/generate_mock_data.py
python scripts/cli.py run --run-config configs/run_spec.yaml --fe-config configs/fe_config.yaml --constraints configs/constraints.yaml
```
