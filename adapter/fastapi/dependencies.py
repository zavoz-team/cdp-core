from collections.abc import Callable
from typing import Any, TypeVar

from fastapi import Depends

T = TypeVar('T')

_stubs: dict[type[Any], Callable[..., Any]] = {}


def get_stub(interface: type[T]) -> Callable[..., T]:
    if interface not in _stubs:

        def stub() -> T:
            raise NotImplementedError

        _stubs[interface] = stub
    return _stubs[interface]


def provide(interface: type[T]) -> Any:
    return Depends(get_stub(interface))
