from tsmp.orchestration.pipeline import Pipeline

def test_pipeline_runs():
    pl = Pipeline(model_capability_path="configs/model_capability.yaml")
    out = pl.run("configs/run_spec.yaml", "configs/fe_config.yaml", "configs/constraints.yaml")
    assert "run_id" in out and out["predictions_rows"] > 0
