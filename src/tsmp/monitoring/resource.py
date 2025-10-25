import time, psutil, os
from typing import Dict, Any

class ResourceMonitor:
    def __init__(self) -> None:
        self.proc = psutil.Process(os.getpid())

    def snapshot(self) -> Dict[str, Any]:
        cpu = self.proc.cpu_percent(interval=0.1)
        mem = self.proc.memory_info().rss / (1024**2)
        return {"cpu_percent": cpu, "memory_mb": mem}
