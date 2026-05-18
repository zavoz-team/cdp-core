import json
from unittest.mock import AsyncMock, MagicMock
import pytest
from datetime import datetime, timezone
from adapter.worker.handlers import WorkerMessageRouter, EventMapper, ProcessingRetryError
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
async def test_dispatch_processed_outcome(router, mock_service):
    # Arrange
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.value = json.dumps({
        "event_id": "evt_1",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"email": ["test@example.com"]}
    }).encode('utf-8')
    
    mock_service.process_event.return_value = ProcessEventResult(
        event_id="evt_1",
        outcome=ProcessEventOutcome.PROCESSED,
        raw_event_status=RawEventProcessingStatus.PROCESSED,
        customer_id="cust_1"
    )

    # Act
    await router.dispatch(message)

    # Assert
    mock_service.process_event.assert_called_once()
    # No exception raised means commit will happen

@pytest.mark.asyncio
async def test_dispatch_dlq_outcome(router, mock_service, mock_producer):
    # Arrange
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.partition = 0
    message.offset = 100
    message.value = json.dumps({
        "event_id": "evt_bad",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z"
    }).encode('utf-8')
    
    mock_service.process_event.return_value = ProcessEventResult(
        event_id="evt_bad",
        outcome=ProcessEventOutcome.SEND_TO_DLQ,
        raw_event_status=RawEventProcessingStatus.SENT_TO_DLQ,
        reason="invalid_payload"
    )

    # Act
    await router.dispatch(message)

    # Assert
    mock_producer.send_and_wait.assert_called_once()
    args, kwargs = mock_producer.send_and_wait.call_args
    assert args[0] == "cdp.events.dlq"
    payload = json.loads(kwargs['value'].decode('utf-8'))
    assert payload['reason'] == "invalid_payload"

@pytest.mark.asyncio
async def test_dispatch_failed_outcome_raises_retry(router, mock_service):
    # Arrange
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.value = json.dumps({
        "event_id": "evt_fail",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z"
    }).encode('utf-8')
    
    mock_service.process_event.return_value = ProcessEventResult(
        event_id="evt_fail",
        outcome=ProcessEventOutcome.FAILED,
        raw_event_status=RawEventProcessingStatus.FAILED,
        reason="db_error"
    )

    # Act & Assert
    with pytest.raises(ProcessingRetryError):
        await router.dispatch(message)
