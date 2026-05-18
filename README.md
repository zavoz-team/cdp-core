# cdp-core

Основной сервис CDP на FastAPI.

Ответственность:
- вычитывать события из Kafka
- выполнять бизнес-логику и identity resolution
- обновлять Customer 360 Lite в PostgreSQL
- отдавать API для профилей, сегментов и экспортов

## Локальный запуск (Kafka)
Для полноценной проверки потока (External event -> Kafka -> cdp-core) необходимо поднять инфраструктуру:
```bash
make compose-up
```
Это запустит PostgreSQL и Kafka. Готовность Kafka можно проверить по состоянию контейнера (healthcheck).
Переменные для настройки Kafka в `.env`:
- `CDP_CORE_KAFKA_BOOTSTRAP_SERVERS` (default: localhost:9092)
- `CDP_CORE_KAFKA_EVENTS_TOPIC` (default: cdp.internal.events)
