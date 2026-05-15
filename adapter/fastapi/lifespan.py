import contextlib
from collections.abc import AsyncGenerator

from fastapi import FastAPI

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    async with contextlib.AsyncExitStack() as stack:
        # TODO: инициализировать и примонтировать сюда долгоживущие ресурсы (например, пул БД, HTTP-клиент)
        # используя app.state и stack.enter_async_context
        yield
