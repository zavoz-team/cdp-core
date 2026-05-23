Customer Data Platform это ядро системы. Обрабатывает события из Kafka, выполняет identity resolution, строит Customer 360 профили и предоставляет REST API

## Архитектура

```
domain/        — сущности, value objects, доменные ошибки, правила
usecase/       — бизнес-логика, порты (интерфейсы)
adapter/       — FastAPI (HTTP), Kafka worker, конфигурация
repository/    — реализации портов (PostgreSQL)
migrations/    — Alembic миграции
```

Зависимости направлены внутрь: `adapter/` и `repository/` -> `usecase/` -> `domain/`

## Возможности

- Приём и обработка событий из Kafka (consumer group)
- Identity resolution (объединение анонимных и известных профилей)
- Customer 360 Lite профили в PostgreSQL
- Сегментация клиентов
- REST API: профили, сегменты, экспорты, джобы
- Dead Letter Queue для невалидных событий

## Стек

- Python 3.13+, FastAPI, uvicorn
- PostgreSQL 17, SQLAlchemy, asyncpg
- Apache Kafka, aiokafka
- Alembic (миграции)
- uv (пакетный менеджер)

## Быстрый старт

### Запуск всего стека

```bash
docker compose up -d
```

Это поднимет:

| Сервис | Описание |
|--------|----------|
| `cdp-core-postgres` | PostgreSQL 17 |
| `cdp-core-kafka` | Apache Kafka |
| `cdp-core-kafka-setup` | Создание топиков |
| `cdp-core-migrations` | Alembic миграции |
| `cdp-core-api` | HTTP API (порт 8000) |
| `cdp-core-worker` | Kafka consumer |

### Проверка

```bash
curl http://localhost:8000/api/v1/health
```

## Локальная разработка (без Docker)

```bash
# Установить зависимости
make install

# Поднять инфраструктуру
docker compose up -d cdp-core-postgres cdp-core-kafka cdp-core-kafka-setup

# Применить миграции
uv run alembic upgrade head

# Запустить API
make run

# Запустить worker
uv run python -m adapter.worker.main
```

## Конфигурация

Базовый конфиг: `config/app.yaml`

Переопределение через переменные окружения:

| Переменная | Описание | Default |
|-----------|----------|---------|
| `CDP_CORE_POSTGRES_HOST` | Хост PostgreSQL | localhost |
| `CDP_CORE_POSTGRES_PORT` | Порт PostgreSQL | 5432 |
| `CDP_CORE_POSTGRES_DATABASE` | Имя БД | cdp_core |
| `CDP_CORE_POSTGRES_USER` | Пользователь | cdp_core |
| `CDP_CORE_POSTGRES_PASSWORD` | Пароль | — |
| `CDP_CORE_KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers | localhost:9092 |
| `CDP_CORE_KAFKA_EVENTS_V1_TOPIC` | Топик событий | cdp.events.v1 |
| `CDP_CORE_KAFKA_EVENTS_DLQ_TOPIC` | DLQ топик | cdp.events.dlq |

## Команды

```bash
make install          # Установить зависимости
make run              # Запустить API
make test             # Все тесты
make test-unit        # Unit тесты
make test-integration # Integration тесты
make lint             # Линтер (ruff)
make typecheck        # Проверка типов (mypy)
make pre-commit       # lint + typecheck + test
```

## API

Base URL: `http://localhost:8000/api/v1`

- `GET /health` — healthcheck
- `GET /profiles` — профили клиентов
- `GET /segments` — сегменты
- `POST /exports` — экспорт данных
- `GET /jobs` — статус джобов

## Лицензия

См. [LICENSE](LICENSE)
