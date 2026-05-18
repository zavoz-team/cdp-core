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
    return lambda: datetime(2026, 5, 18, 10, 0, 0, tzinfo=timezone.utc)

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
async def test_dlq_message_content_on_identity_conflict(router, mock_service, mock_producer):
    # Arrange
    raw_data = {
        "event_id": "evt_conflict_123",
        "event_type": "page_view",
        "source": "web_store",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"email": ["a@ex.com", "b@ex.com"]},
        "trace_context": {"span_id": "span_1"}
    }
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.partition = 1
    message.offset = 500
    message.value = json.dumps(raw_data).encode('utf-8')
    
    mock_service.process_event.return_value = ProcessEventResult.send_to_dlq(
        "evt_conflict_123", "identity_conflict"
    )

    # Act
    await router.dispatch(message)

    # Assert
    mock_producer.send_and_wait.assert_called_once()
    args, kwargs = mock_producer.send_and_wait.call_args
    assert args[0] == "cdp.events.dlq"
    
    dlq_payload = json.loads(kwargs['value'].decode('utf-8'))
    assert dlq_payload['reason'] == "identity_conflict"
    assert dlq_payload['event_id'] == "evt_conflict_123"
    assert dlq_payload['source'] == "web_store"
    assert dlq_payload['failed_at'] == "2026-05-18T10:00:00+00:00"
    assert dlq_payload['trace_context']['span_id'] == "span_1"
    assert dlq_payload['metadata']['kafka_offset'] == 500
    assert dlq_payload['original_event'] == message.value.decode('utf-8')

@pytest.mark.asyncio
async def test_dlq_message_content_on_mapping_error(router, mock_producer):
    # Arrange: invalid JSON
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.partition = 0
    message.offset = 1
    message.value = b"not a json"
    
    # Act
    await router.dispatch(message)

    # Assert
    mock_producer.send_and_wait.assert_called_once()
    dlq_payload = json.loads(mock_producer.send_and_wait.call_args[1]['value'].decode('utf-8'))
    assert dlq_payload['reason'] == "invalid_event_structure"
    assert "invalid json" in dlq_payload['error_message']
    assert dlq_payload['original_event'] == "not a json"

@pytest.mark.asyncio
async def test_offset_not_committed_if_dlq_publish_fails(router, mock_service, mock_producer):
    # Arrange
    message = MagicMock()
    message.topic = "cdp.events.v1"
    message.value = json.dumps({
        "event_id": "evt_1", "event_type": "page_view", "source": "web", "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"email": ["a@ex.com"]}
    }).encode('utf-8')
    
    mock_service.process_event.return_value = ProcessEventResult.send_to_dlq("evt_1", "some_reason")
    
    # Simulate Kafka producer failure
    mock_producer.send_and_wait.side_effect = Exception("kafka down")

    # Act & Assert
    with pytest.raises(ProcessingRetryError):
        await router.dispatch(message)
