# cdp-core

Основной сервис CDP на FastAPI.

Ответственность:
- вычитывать события из Kafka
- выполнять бизнес-логику и identity resolution
- обновлять Customer 360 Lite в PostgreSQL
- отдавать API для профилей, сегментов и экспортов
