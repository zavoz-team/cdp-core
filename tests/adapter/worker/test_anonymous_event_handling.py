import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from adapter.observability.noop import NoopLogger, NoopMetrics, NoopTracer
from adapter.worker.handlers import EventMapper, WorkerMessageRouter
from domain.event import RawEventProcessingStatus
from usecase.dto import ProcessEventOutcome, ProcessEventResult


@pytest.fixture
def mock_producer():
    return AsyncMock()


@pytest.fixture
def mock_service():
    return AsyncMock()


@pytest.fixture
def now_provider():
    return lambda: datetime(2026, 5, 18, tzinfo=timezone.utc)


@pytest.fixture
def router(mock_producer, mock_service, now_provider):
    return WorkerMessageRouter(
        producer=mock_producer,
        process_event_service=mock_service,
        event_mapper=EventMapper(),
        now_provider=now_provider,
        events_v1_topic='cdp.events.v1',
        events_dlq_topic='cdp.events.dlq',
        logger=NoopLogger(),
        tracer=NoopTracer(),
        metrics=NoopMetrics(),
    )


@pytest.mark.asyncio
async def test_anonymous_event_is_ignored_correctly(
    router, mock_service, mock_producer
):
    # Arrange: event with only anonymous_id
    message = MagicMock()
    message.topic = 'cdp.events.v1'
    message.value = json.dumps(
        {
            'event_id': 'evt_anon',
            'event_type': 'page_view',
            'source': 'web',
            'occurred_at': '2026-05-18T10:00:00Z',
            'identifiers': {'anonymous_id': 'anon_123'},
        }
    ).encode('utf-8')

    # Use case returns IGNORED_ANONYMOUS
    mock_service.process_event.return_value = ProcessEventResult(
        event_id='evt_anon',
        outcome=ProcessEventOutcome.IGNORED_ANONYMOUS,
        raw_event_status=RawEventProcessingStatus.IGNORED_ANONYMOUS,
    )

    # Act
    await router.dispatch(message)

    # Assert
    # 1. Service was called
    mock_service.process_event.assert_called_once()

    # 2. DLQ was NOT used
    mock_producer.send_and_wait.assert_not_called()

    # 3. No exception raised (caller will commit offset)

    # Verify RawEvent construction in Mapper through service call
    called_raw_event = mock_service.process_event.call_args[0][0]
    assert called_raw_event.identifiers.anonymous_id == 'anon_123'
    assert not called_raw_event.identifiers.has_known
    assert called_raw_event.identifiers.is_anonymous_only


@pytest.mark.asyncio
async def test_anonymous_event_does_not_trigger_dlq_on_success(
    router, mock_service, mock_producer
):
    message = MagicMock()
    message.topic = 'cdp.events.v1'
    message.value = json.dumps(
        {
            'event_id': 'evt_anon',
            'event_type': 'page_view',
            'source': 'web',
            'occurred_at': '2026-05-18T10:00:00Z',
            'identifiers': {'anonymous_id': 'anon_123'},
        }
    ).encode('utf-8')

    mock_service.process_event.return_value = ProcessEventResult(
        event_id='evt_anon',
        outcome=ProcessEventOutcome.IGNORED_ANONYMOUS,
        raw_event_status=RawEventProcessingStatus.IGNORED_ANONYMOUS,
    )

    await router.dispatch(message)

    mock_producer.send_and_wait.assert_not_called()
