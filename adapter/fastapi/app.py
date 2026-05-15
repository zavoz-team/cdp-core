from fastapi import FastAPI

from adapter.fastapi.lifespan import lifespan
from adapter.fastapi.provider_registry import registry
from adapter.fastapi.routes.exports import router as exports_router
from adapter.fastapi.routes.health import router as health_router
from adapter.fastapi.routes.profiles import router as profiles_router
from adapter.fastapi.routes.segments import router as segments_router


def create_app() -> FastAPI:
    app = FastAPI(
        title='CDP Core API',
        lifespan=lifespan,
    )
    app.include_router(health_router, prefix='/api/v1', tags=['Health'])
    app.include_router(profiles_router, prefix='/api/v1')
    app.include_router(segments_router, prefix='/api/v1')
    app.include_router(exports_router, prefix='/api/v1')
    registry.setup_app(app)
    return app
