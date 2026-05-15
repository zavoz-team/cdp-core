from fastapi import FastAPI

from adapter.fastapi.lifespan import lifespan
from adapter.fastapi.provider_registry import registry

def create_app() -> FastAPI:
    app = FastAPI(
        title="CDP Core API",
        lifespan=lifespan,
    )
    registry.setup_app(app)
    return app
