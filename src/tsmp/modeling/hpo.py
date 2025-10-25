from typing import Dict, Any

class NoOpHPO:
    def run(self, search_space: Dict[str, Any]) -> Dict[str, Any]:
        # return defaults for demo
        return {k:v[0] if isinstance(v, list) else v for k,v in search_space.items()}
