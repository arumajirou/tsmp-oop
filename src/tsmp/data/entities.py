from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class Observation:
    unique_id: str
    ds: datetime
    y: float
