from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class Event:
    name: str
    payload: Dict[str, Any]
