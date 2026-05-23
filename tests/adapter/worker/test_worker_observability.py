import json
import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from adapter.worker.handlers import EventMapper, WorkerMessageRouter
from usecase.dto import ProcessEventResult


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
def router(mock_producer, mock_service, now_provider):
    return WorkerMessageRouter(
        producer=mock_producer,
        process_event_service=mock_service,
        event_mapper=EventMapper(),
        now_provider=now_provider,
        events_v1_topic='cdp.events.v1',
        events_dlq_topic='cdp.events.dlq',
    )


@pytest.mark.asyncio
async def test_logging_on_success(router, mock_service, caplog):
    caplog.set_level(logging.INFO)

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
    assert 'raw_event_mapped event_id=evt_123' in caplog.text
    assert 'customer_profile_updated event_id=evt_123' in caplog.text
    assert 'customer_id=cust_1' in caplog.text
    assert 'processing_status=processed' in caplog.text
    assert 'segment_membership_updated' in caplog.text


@pytest.mark.asyncio
async def test_logging_on_dlq(router, mock_service, mock_producer, caplog):
    caplog.set_level(logging.WARNING)

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
    assert 'event_sent_to_dlq event_id=evt_bad' in caplog.text
    assert 'reason=identity_conflict' in caplog.text
    assert 'processing_status=sent_to_dlq' in caplog.text


@pytest.mark.asyncio
async def test_logging_on_mapping_error(router, caplog):
    caplog.set_level(logging.ERROR)

    # Arrange: invalid message
    message = MagicMock()
    message.topic = 'cdp.events.v1'
    message.value = b'invalid'

    # Act
    await router.dispatch(message)

    # Assert
    assert 'event_processing_failed reason=mapping_error' in caplog.text
