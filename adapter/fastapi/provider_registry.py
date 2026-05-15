from collections.abc import Callable
from typing import Any, TypeVar

from fastapi import FastAPI

T = TypeVar('T')


class ProviderRegistry:
    def __init__(self) -> None:
        self.providers: dict[Any, Callable[..., Any]] = {}

    def register(
        self, interface: type[T]
    ) -> Callable[[Callable[..., T]], Callable[..., T]]:
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            self.providers[interface] = func
            return func

        return decorator

    def setup_app(self, app: FastAPI) -> None:
        for interface, provider in self.providers.items():
            app.dependency_overrides[interface] = provider


registry = ProviderRegistry()
