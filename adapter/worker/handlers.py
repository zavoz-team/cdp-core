import json
import logging
from dataclasses import dataclass
from datetime import datetime
from aiokafka import AIOKafkaProducer
from usecase.process_event import EventProcessingService
from usecase.dto import ProcessEventResult, ProcessEventOutcome
from domain.event import RawEvent
from domain.identity import CustomerIdentifiers, KnownIdentifier, KnownIdentifierType

logger = logging.getLogger(__name__)


class UnknownWorkerTopicError(Exception):
    pass


class ProcessingRetryError(Exception):
    """Raised when processing failed and should be retried (no offset commit)"""
    pass


class EventMapper:
    def to_raw_event(self, message) -> RawEvent:
        try:
            data = json.loads(message.value)
        except json.JSONDecodeError as e:
            raise ValueError(f"invalid json: {e}")

        identifiers_raw = data.get('identifiers', {})
        known = []
        # Filter and map known identifiers
        for id_type, values in identifiers_raw.items():
            if id_type == 'anonymous_id':
                continue
            try:
                kind = KnownIdentifierType(id_type)
                if isinstance(values, list):
                    for val in values:
                        known.append(KnownIdentifier(identifier_type=kind, value=str(val)))
                else:
                    known.append(KnownIdentifier(identifier_type=kind, value=str(values)))
            except ValueError:
                continue

        return RawEvent(
            event_id=str(data['event_id']),
            source=str(data['source']),
            payload=data.get('payload', {}),
            identifiers=CustomerIdentifiers(
                anonymous_id=identifiers_raw.get('anonymous_id'),
                known=tuple(known)
            ),
            occurred_at=datetime.fromisoformat(data['occurred_at']),
            created_at=datetime.fromisoformat(data.get('created_at', data['occurred_at']))
        )


class WorkerMessageRouter:
    def __init__(
        self,
        producer: AIOKafkaProducer,
        process_event_service: EventProcessingService,
        event_mapper: EventMapper,
        now_provider,
        events_v1_topic: str,
        events_dlq_topic: str
    ) -> None:
        self._producer = producer
        self._process_event_service = process_event_service
        self._event_mapper = event_mapper
        self._now_provider = now_provider
        self._events_v1_topic = events_v1_topic
        self._events_dlq_topic = events_dlq_topic

    async def dispatch(self, message) -> None:
        if message.topic == self._events_v1_topic:
            await self._handle_cdp_event(message)
            return

        raise UnknownWorkerTopicError(f"No handler for topic {message.topic}")

    async def send(self, topic_name: str, payload, key=None, headers=None) -> None:
        value = json.dumps(payload).encode('utf-8')
        await self._producer.send_and_wait(topic_name, value=value, key=key, headers=headers)

    async def _handle_cdp_event(self, message) -> None:
        try:
            raw_event = self._event_mapper.to_raw_event(message)
        except Exception as e:
            logger.error(f'failed to map event: {e}')
            await self._send_to_dlq(message, f"mapping_error: {e}")
            return

        result = await self._process_event_service.process_event(raw_event)
        
        if result.outcome == ProcessEventOutcome.PROCESSED:
            logger.info(f"event {result.event_id} processed for customer {result.customer_id}")
            return
            
        if result.outcome == ProcessEventOutcome.IGNORED_ANONYMOUS:
            logger.info(f"event {result.event_id} ignored (anonymous)")
            return

        if result.outcome == ProcessEventOutcome.SEND_TO_DLQ:
             await self._send_to_dlq(message, result.reason or "unknown_failure")
             return
             
        if result.outcome == ProcessEventOutcome.FAILED:
            # According to rules: failed before stable outcome -> no commit
            # We raise exception to prevent commit in worker loop
            raise ProcessingRetryError(f"processing failed for event {result.event_id}: {result.reason}")

    async def _send_to_dlq(self, message, reason: str) -> None:
        payload = {
            "original_message": {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "value": message.value.decode('utf-8', errors='replace') if message.value else None,
            },
            "reason": reason,
            "metadata": {
                "timestamp": self._now_provider().isoformat()
            }
        }
        try:
            await self.send(self._events_dlq_topic, payload)
            logger.info(f"message sent to DLQ: {reason}")
        except Exception as e:
            # If DLQ publish fails, we MUST retry (no commit)
            raise ProcessingRetryError(f"failed to send to DLQ: {e}") from e
