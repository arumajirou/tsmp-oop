from typing import Any, Dict, Type, TypeVar, Callable

T = TypeVar("T")

class Container:
    """Minimal DI container with factories and singletons."""
    def __init__(self) -> None:
        self._singletons: Dict[str, Any] = {}
        self._factories: Dict[str, Callable[[], Any]] = {}

    def register_singleton(self, key: str, instance: Any) -> None:
        self._singletons[key] = instance

    def register_factory(self, key: str, factory: Callable[[], Any]) -> None:
        self._factories[key] = factory

    def resolve(self, key: str) -> Any:
        if key in self._singletons:
            return self._singletons[key]
        if key in self._factories:
            return self._factories[key]()
        raise KeyError(f"Service not found: {key}")
