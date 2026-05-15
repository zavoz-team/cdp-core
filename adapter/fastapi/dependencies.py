from typing import Any, TypeVar

from fastapi import Depends

T = TypeVar("T")

def Provide(interface: type[T]) -> Any:
    return Depends(interface)
