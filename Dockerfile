FROM python:3.13-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . .
RUN uv sync --frozen --no-dev


FROM base AS api

EXPOSE 8000
CMD ["uv", "run", "uvicorn", "adapter.fastapi.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]


FROM base AS worker

CMD ["uv", "run", "python", "-m", "adapter.worker.main"]


FROM base AS migrations

CMD ["uv", "run", "alembic", "upgrade", "head"]
