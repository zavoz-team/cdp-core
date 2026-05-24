import contextlib
from collections.abc import AsyncGenerator

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from adapter.config.loader import load_config
from adapter.observability.factory import build_observability


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    config = load_config()
    obs = build_observability(config)

    db_url = f'postgresql+asyncpg://{config.postgres.user}:{config.postgres.password}@{config.postgres.host}:{config.postgres.port}/{config.postgres.database}'
    engine = create_async_engine(
        db_url,
        pool_size=config.postgres.pool_max_size,
        max_overflow=0,
        pool_timeout=config.postgres.connect_timeout_seconds,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    app.state.config = config
    app.state.obs = obs
    app.state.engine = engine
    app.state.session_factory = session_factory

    async with contextlib.AsyncExitStack() as stack:
        http_client = await stack.enter_async_context(httpx.AsyncClient())
        app.state.http_client = http_client

        yield

    await engine.dispose()
    obs.shutdown()
