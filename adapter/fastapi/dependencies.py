from typing import Any, TypeVar

from fastapi import Depends

T = TypeVar('T')


def provide(interface: type[T]) -> Any:
    return Depends(interface)
