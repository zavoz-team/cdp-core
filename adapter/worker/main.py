import asyncio
import signal
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore[import-untyped]
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from adapter.config.loader import load_config
from adapter.observability.factory import build_observability
from adapter.worker.handler import ensure_topics_exist
from adapter.worker.handlers import EventMapper, WorkerMessageRouter
from adapter.worker.worker import KafkaWorker
from repository.uow import SqlAlchemyEventProcessingUnitOfWork
from usecase.process_event import EventProcessingService


async def main() -> None:
    config = load_config()
    obs = build_observability(config)
    logger = obs.logger
    tracer = obs.tracer
    metrics = obs.metrics

    # 0. Startup checks
    try:
        ensure_topics_exist(config, logger)
    except RuntimeError as e:
        logger.error(f'startup check failed: {e}')
        return

    # 1. Database setup
    db_url = f'postgresql+asyncpg://{config.postgres.user}:{config.postgres.password}@{config.postgres.host}:{config.postgres.port}/{config.postgres.database}'
    engine = create_async_engine(db_url, pool_size=config.postgres.pool_max_size)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # 2. Domain services setup
    uow = SqlAlchemyEventProcessingUnitOfWork(session_factory, logger, tracer)
    process_event_service = EventProcessingService(
        unit_of_work=uow,
        customer_id_generator=lambda: f'cust_{uuid.uuid4().hex}',
        now_provider=lambda: datetime.now(timezone.utc),
        logger=logger,
        tracer=tracer,
        metrics=metrics,
    )

    # 3. Kafka setup
    consumer = AIOKafkaConsumer(
        config.kafka.events_v1_topic,
        bootstrap_servers=config.kafka.bootstrap_servers,
        group_id='cdp-core-worker',
        enable_auto_commit=False,
        auto_offset_reset='earliest',
    )

    producer = AIOKafkaProducer(bootstrap_servers=config.kafka.bootstrap_servers)

    # 4. Router & Worker setup
    router = WorkerMessageRouter(
        producer=producer,
        process_event_service=process_event_service,
        event_mapper=EventMapper(),
        now_provider=lambda: datetime.now(timezone.utc),
        events_v1_topic=config.kafka.events_v1_topic,
        events_dlq_topic=config.kafka.events_dlq_topic,
        logger=logger,
        tracer=tracer,
        metrics=metrics,
    )

    worker = KafkaWorker(
        consumer=consumer,
        router=router,
        logger=logger,
        tracer=tracer,
        metrics=metrics,
    )

    # 5. Resources management & Shutdown
    await consumer.start()
    await producer.start()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)

    try:
        await worker.run()
    finally:
        logger.info('shutting down resources')
        await producer.stop()
        await consumer.stop()
        await engine.dispose()
        obs.shutdown()
        logger.info('shutdown complete')


if __name__ == '__main__':
    asyncio.run(main())
