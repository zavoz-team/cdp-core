import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from adapter.observability.noop import NoopMetrics, NoopTracer
from adapter.worker.handlers import EventMapper, WorkerMessageRouter
from usecase.dto import ProcessEventResult
from usecase.interface import Attrs


class SpyLogger:
    """Логгер-шпион для проверки вызовов"""

    def __init__(self) -> None:
        self.messages: list[tuple[str, str, Attrs | None]] = []

    def debug(self, message: str, attrs: Attrs | None = None) -> None:
        self.messages.append(('debug', message, attrs))

    def info(self, message: str, attrs: Attrs | None = None) -> None:
        self.messages.append(('info', message, attrs))

    def warning(self, message: str, attrs: Attrs | None = None) -> None:
        self.messages.append(('warning', message, attrs))

    def error(self, message: str, attrs: Attrs | None = None) -> None:
        self.messages.append(('error', message, attrs))

    def has_message(self, level: str, substring: str) -> bool:
        return any(level == lvl and substring in msg for lvl, msg, _ in self.messages)

    def has_attr(self, level: str, key: str, value: object) -> bool:
        return any(
            level == lvl and attrs is not None and attrs.get(key) == value
            for lvl, _, attrs in self.messages
        )


@pytest.fixture
def mock_producer():
    return AsyncMock()


@pytest.fixture
def mock_service():
    return AsyncMock()


@pytest.fixture
def now_provider():
    return lambda: datetime(2026, 5, 18, 10, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def spy_logger():
    return SpyLogger()


@pytest.fixture
def router(mock_producer, mock_service, now_provider, spy_logger):
    return WorkerMessageRouter(
        producer=mock_producer,
        process_event_service=mock_service,
        event_mapper=EventMapper(),
        now_provider=now_provider,
        events_v1_topic='cdp.events.v1',
        events_dlq_topic='cdp.events.dlq',
        logger=spy_logger,
        tracer=NoopTracer(),
        metrics=NoopMetrics(),
    )


@pytest.mark.asyncio
async def test_logging_on_success(router, mock_service, spy_logger):
    # Arrange
    message = MagicMock()
    message.topic = 'cdp.events.v1'
    message.value = json.dumps(
        {
            'event_id': 'evt_123',
            'event_type': 'purchase',
            'source': 'mobile',
            'occurred_at': '2026-05-18T10:00:00Z',
            'identifiers': {'email': ['user@example.com']},
        }
    ).encode('utf-8')

    mock_service.process_event.return_value = ProcessEventResult.processed(
        'evt_123', 'cust_1'
    )

    # Act
    await router.dispatch(message)

    # Assert
    assert spy_logger.has_message('info', 'raw event mapped')
    assert spy_logger.has_attr('info', 'event_id', 'evt_123')
    assert spy_logger.has_message('info', 'event processed')
    assert spy_logger.has_attr('info', 'customer_id', 'cust_1')


@pytest.mark.asyncio
async def test_logging_on_dlq(router, mock_service, mock_producer, spy_logger):
    # Arrange
    message = MagicMock()
    message.topic = 'cdp.events.v1'
    message.value = json.dumps(
        {
            'event_id': 'evt_bad',
            'event_type': 'purchase',
            'source': 'web',
            'occurred_at': '2026-05-18T10:00:00Z',
            'identifiers': {'email': ['bad@ex.com']},
        }
    ).encode('utf-8')

    mock_service.process_event.return_value = ProcessEventResult.send_to_dlq(
        'evt_bad', 'identity_conflict'
    )

    # Act
    await router.dispatch(message)

    # Assert
    assert spy_logger.has_message('warning', 'event sent to dlq')
    assert spy_logger.has_attr('warning', 'event_id', 'evt_bad')
    assert spy_logger.has_attr('warning', 'reason', 'identity_conflict')


@pytest.mark.asyncio
async def test_logging_on_mapping_error(router, spy_logger):
    # Arrange: invalid message
    message = MagicMock()
    message.topic = 'cdp.events.v1'
    message.value = b'invalid'

    # Act
    await router.dispatch(message)

    # Assert
    assert spy_logger.has_message('error', 'event processing failed')
    assert spy_logger.has_attr('error', 'reason', 'mapping_error')
