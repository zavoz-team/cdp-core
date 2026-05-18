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

def create_kafka_msg(payload):
    msg = MagicMock()
    msg.topic = "cdp.events.v1"
    msg.partition = 0
    msg.offset = 123
    msg.value = json.dumps(payload).encode('utf-8')
    return msg

@pytest.mark.asyncio
async def test_process_page_view_success(router, mock_service):
    # Arrange
    msg = create_kafka_msg({
        "event_id": "evt_pv_1",
        "event_type": "page_view",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"email": ["user@example.com"]}
    })
    
    mock_service.process_event.return_value = ProcessEventResult.processed("evt_pv_1", "cust_1")

    # Act
    await router.dispatch(msg)

    # Assert
    mock_service.process_event.assert_called_once()
    raw_event = mock_service.process_event.call_args[0][0]
    assert raw_event.event_type == "page_view"
    assert raw_event.identifiers.has_known

@pytest.mark.asyncio
async def test_process_purchase_success(router, mock_service):
    # Arrange
    msg = create_kafka_msg({
        "event_id": "evt_p_1",
        "event_type": "purchase",
        "source": "pos",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"phone": ["+79001234567"]},
        "payload": {
            "order_id": "order_123",
            "amount": 1500.50,
            "currency": "RUB"
        }
    })
    
    mock_service.process_event.return_value = ProcessEventResult.processed("evt_p_1", "cust_2")

    # Act
    await router.dispatch(msg)

    # Assert
    mock_service.process_event.assert_called_once()
    raw_event = mock_service.process_event.call_args[0][0]
    assert raw_event.event_type == "purchase"
    assert raw_event.payload["order_id"] == "order_123"

@pytest.mark.asyncio
async def test_process_purchase_invalid_payload_goes_to_dlq(router, mock_service, mock_producer):
    # Arrange
    msg = create_kafka_msg({
        "event_id": "evt_bad_p",
        "event_type": "purchase",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"email": ["bad@example.com"]},
        "payload": {"amount": -10} # Missing order_id and invalid amount
    })
    
    mock_service.process_event.return_value = ProcessEventResult.send_to_dlq("evt_bad_p", "invalid_purchase_payload")

    # Act
    await router.dispatch(msg)

    # Assert
    mock_producer.send_and_wait.assert_called_once()
    assert "invalid_purchase_payload" in mock_producer.send_and_wait.call_args[1]['value'].decode()

@pytest.mark.asyncio
async def test_process_identity_conflict_goes_to_dlq(router, mock_service, mock_producer):
    # Arrange
    msg = create_kafka_msg({
        "event_id": "evt_conflict",
        "event_type": "page_view",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"email": ["a@ex.com", "b@ex.com"]}
    })
    
    mock_service.process_event.return_value = ProcessEventResult.send_to_dlq("evt_conflict", "identity_conflict")

    # Act
    await router.dispatch(msg)

    # Assert
    mock_producer.send_and_wait.assert_called_once()
    assert "identity_conflict" in mock_producer.send_and_wait.call_args[1]['value'].decode()

@pytest.mark.asyncio
async def test_duplicate_event_id_handled_by_service(router, mock_service):
    # Arrange
    msg = create_kafka_msg({
        "event_id": "evt_dup",
        "event_type": "page_view",
        "source": "web",
        "occurred_at": "2026-05-18T10:00:00Z",
        "identifiers": {"email": ["dup@ex.com"]}
    })
    
    # Service returns PROCESSED but with None customer_id (as per usecase code for duplicates)
    mock_service.process_event.return_value = ProcessEventResult.processed("evt_dup", None)

    # Act
    await router.dispatch(msg)

    # Assert
    mock_service.process_event.assert_called_once()
    # No DLQ, offset will commit
