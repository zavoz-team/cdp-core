from fastapi import FastAPI

from adapter.fastapi.lifespan import lifespan
from adapter.fastapi.provider_registry import registry
from adapter.fastapi.routes.health import router as health_router


def create_app() -> FastAPI:
    app = FastAPI(
        title='CDP Core API',
        lifespan=lifespan,
    )
    app.include_router(health_router, prefix='/api/v1', tags=['Health'])
    registry.setup_app(app)
    return app
