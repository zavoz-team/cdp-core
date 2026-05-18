import asyncio
import logging
import signal
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from adapter.config.loader import load_config
from adapter.worker.handlers import WorkerMessageRouter
from adapter.worker.worker import KafkaWorker
from domain.event import RawEvent
from domain.identity import (
    CustomerIdentifiers,
    KnownIdentifier,
    KnownIdentifierType,
)
from repository.uow import SqlAlchemyEventProcessingUnitOfWork
from usecase.process_event import EventProcessingService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventMapper:
    def to_raw_event(self, message) -> RawEvent:
        data = json.loads(message.value)
        
        identifiers = data.get('identifiers', {})
        known = []
        for id_type, values in identifiers.items():
            try:
                kind = KnownIdentifierType(id_type)
                for val in values:
                    known.append(KnownIdentifier(identifier_type=kind, value=val))
            except ValueError:
                continue

        return RawEvent(
            event_id=data['event_id'],
            source=data['source'],
            payload=data.get('payload', {}),
            identifiers=CustomerIdentifiers(
                anonymous_id=identifiers.get('anonymous_id'),
                known=tuple(known)
            ),
            occurred_at=datetime.fromisoformat(data['occurred_at']),
            created_at=datetime.fromisoformat(data.get('created_at', data['occurred_at']))
        )


import json

async def main() -> None:
    config = load_config()
    
    # 1. Database setup
    db_url = f"postgresql+asyncpg://{config.postgres.user}:{config.postgres.password}@{config.postgres.host}:{config.postgres.port}/{config.postgres.database}"
    engine = create_async_engine(db_url, pool_size=config.postgres.pool_max_size)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    
    # 2. Domain services setup
    uow = SqlAlchemyEventProcessingUnitOfWork(session_factory)
    process_event_service = EventProcessingService(
        unit_of_work=uow,
        customer_id_generator=lambda: f"cust_{uuid.uuid4().hex}",
        now_provider=lambda: datetime.now(timezone.utc),
    )
    
    # 3. Kafka setup
    consumer = AIOKafkaConsumer(
        config.kafka.events_v1_topic,
        bootstrap_servers=config.kafka.bootstrap_servers,
        group_id="cdp-core-worker",
        enable_auto_commit=False,
        auto_offset_reset="earliest"
    )
    
    producer = AIOKafkaProducer(
        bootstrap_servers=config.kafka.bootstrap_servers
    )
    
    # 4. Router & Worker setup
    router = WorkerMessageRouter(
        producer=producer,
        process_event_service=process_event_service,
        event_mapper=EventMapper(),
        now_provider=lambda: datetime.now(timezone.utc),
        events_v1_topic=config.kafka.events_v1_topic,
        events_dlq_topic=config.kafka.events_dlq_topic
    )
    
    worker = KafkaWorker(consumer=consumer, router=router)
    
    # 5. Resources management & Shutdown
    await consumer.start()
    await producer.start()
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)
    
    try:
        await worker.run()
    finally:
        logger.info('shutting down resources...')
        # Shutdown in reverse order
        await producer.stop()
        await consumer.stop()
        await engine.dispose()
        logger.info('shutdown complete')

if __name__ == "__main__":
    asyncio.run(main())
