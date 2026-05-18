import json
from unittest.mock import AsyncMock, MagicMock
import pytest
from datetime import datetime, timezone
from adapter.worker.handlers import WorkerMessageRouter, EventMapper
from usecase.dto import ProcessEventResult, ProcessEventOutcome
from domain.event import RawEventProcessingStatus

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
        events_v1_topic="cdp.events.v1",
        events_dlq_topic="cdp.events.dlq"
    )

@pytest.mark.asyncio
async def test_missing_identifiers_goes_to_dlq(router, mock_producer):
    # Missing both known and anonymous_id
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.value = json.dumps({
        "event_id": "evt_1",
        "event_type": "page_view",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {}
    }).encode('utf-8')

    await router.dispatch(message)

    mock_producer.send_and_wait.assert_called_once()
    args, kwargs = mock_producer.send_and_wait.call_args
    payload = json.loads(kwargs['value'].decode('utf-8'))
    assert "invalid_event_structure" in payload['reason']

@pytest.mark.asyncio
async def test_anonymous_id_only_does_not_go_to_dlq(router, mock_service, mock_producer):
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.value = json.dumps({
        "event_id": "evt_1",
        "event_type": "page_view",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"anonymous_id": "anon_123"}
    }).encode('utf-8')
    
    mock_service.process_event.return_value = ProcessEventResult(
        event_id="evt_1",
        outcome=ProcessEventOutcome.IGNORED_ANONYMOUS,
        raw_event_status=RawEventProcessingStatus.IGNORED_ANONYMOUS
    )

    await router.dispatch(message)

    mock_producer.send_and_wait.assert_not_called()
    mock_service.process_event.assert_called_once()

@pytest.mark.asyncio
async def test_unsupported_event_type_goes_to_dlq(router, mock_service, mock_producer):
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.value = json.dumps({
        "event_id": "evt_1",
        "event_type": "unknown_type",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"email": ["test@example.com"]}
    }).encode('utf-8')
    
    mock_service.process_event.return_value = ProcessEventResult(
        event_id="evt_1",
        outcome=ProcessEventOutcome.SEND_TO_DLQ,
        raw_event_status=RawEventProcessingStatus.SENT_TO_DLQ,
        reason="unsupported_event_type"
    )

    await router.dispatch(message)

    mock_producer.send_and_wait.assert_called_once()
    args, kwargs = mock_producer.send_and_wait.call_args
    payload = json.loads(kwargs['value'].decode('utf-8'))
    assert payload['reason'] == "unsupported_event_type"

@pytest.mark.asyncio
async def test_invalid_purchase_payload_goes_to_dlq(router, mock_service, mock_producer):
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.value = json.dumps({
        "event_id": "evt_1",
        "event_type": "purchase",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"email": ["test@example.com"]},
        "payload": {} # Missing order_id
    }).encode('utf-8')
    
    mock_service.process_event.return_value = ProcessEventResult(
        event_id="evt_1",
        outcome=ProcessEventOutcome.SEND_TO_DLQ,
        raw_event_status=RawEventProcessingStatus.SENT_TO_DLQ,
        reason="invalid_purchase_payload"
    )

    await router.dispatch(message)

    mock_producer.send_and_wait.assert_called_once()
    assert "invalid_purchase_payload" in mock_producer.send_and_wait.call_args[1]['value'].decode()

@pytest.mark.asyncio
async def test_unsupported_currency_goes_to_dlq(router, mock_service, mock_producer):
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.value = json.dumps({
        "event_id": "evt_1",
        "event_type": "purchase",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"email": ["test@example.com"]},
        "payload": {"order_id": "123", "amount": 100, "currency": "USD"}
    }).encode('utf-8')
    
    mock_service.process_event.return_value = ProcessEventResult(
        event_id="evt_1",
        outcome=ProcessEventOutcome.SEND_TO_DLQ,
        raw_event_status=RawEventProcessingStatus.SENT_TO_DLQ,
        reason="unsupported_currency"
    )

    await router.dispatch(message)

    mock_producer.send_and_wait.assert_called_once()
    assert "unsupported_currency" in mock_producer.send_and_wait.call_args[1]['value'].decode()
