from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class RunConfig(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    alias: str = "run"
    model_name: str
    dataset_path: str
    horizon: int
    val_size: float = 0.2
    hyperparams: Dict[str, Any] = Field(default_factory=dict)
    tuner_backend: str = "none"
    log_level: str = "INFO"
    instance_type: str = "local"

class FeatureConfig(BaseModel):
    config_name: str = "features"
    base_data: Dict[str, Any]
    features: list[Dict[str, Any]]

class ConstraintRule(BaseModel):
    name: str
    when: Dict[str, Any]
    then: Dict[str, Any]

class ConstraintSpec(BaseModel):
    rules: list[ConstraintRule] = Field(default_factory=list)
