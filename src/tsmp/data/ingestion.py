from dataclasses import dataclass

@dataclass
class IngestionResult:
    dataset: str
    records: int
    status: str
