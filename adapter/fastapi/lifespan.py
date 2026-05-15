import contextlib
from collections.abc import AsyncGenerator

from fastapi import FastAPI


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    async with contextlib.AsyncExitStack() as _stack:
        # TODO: инициализировать и примонтировать сюда долгоживущие ресурсы
        # используя app.state и _stack.enter_async_context
        yield
