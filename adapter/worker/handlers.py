import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from aiokafka import AIOKafkaProducer  # type: ignore[import-untyped]

from domain.error import InvalidEventError
from domain.event import RawEvent, RawEventIdentifiers
from domain.identity import KnownIdentifier, KnownIdentifierType
from usecase.dto import ProcessEventOutcome
from usecase.process_event import EventProcessingService

logger = logging.getLogger(__name__)


def _require_json_string(data: dict[Any, Any], field_name: str) -> str:
    value = data.get(field_name)
    if not isinstance(value, str):
        raise InvalidEventError(f'{field_name} must be a string')
    return value


class UnknownWorkerTopicError(Exception):
    pass


class ProcessingRetryError(Exception):
    """Raised when processing failed and should be retried (no offset commit)"""

    pass


class EventMapper:
    def to_raw_event(self, message: Any) -> RawEvent:
        try:
            data = json.loads(message.value)
        except json.JSONDecodeError as e:
            raise InvalidEventError(f'invalid json: {e}')

        if not isinstance(data, dict):
            raise InvalidEventError('event must be a json object')

        for field in ['event_id', 'event_type', 'source', 'occurred_at']:
            if field not in data:
                raise InvalidEventError(f'missing required field: {field}')

        identifiers_raw = data.get('identifiers', {})
        if not isinstance(identifiers_raw, dict):
            raise InvalidEventError('identifiers must be a json object')

        known: list[KnownIdentifier] = []
        anonymous_id = identifiers_raw.get('anonymous_id')

        for id_type, values in identifiers_raw.items():
            if id_type == 'anonymous_id':
                continue
            try:
                kind = KnownIdentifierType(id_type)
                if isinstance(values, list):
                    for val in values:
                        known.append(
                            KnownIdentifier(identifier_type=kind, value=str(val))
                        )
                elif values is not None:
                    known.append(
                        KnownIdentifier(identifier_type=kind, value=str(values))
                    )
            except ValueError:
                continue

        now = datetime.now(timezone.utc)
        occurred_at_raw = _require_json_string(data, 'occurred_at')
        created_at_raw = data.get('created_at', occurred_at_raw)
        if not isinstance(created_at_raw, str):
            raise InvalidEventError('created_at must be a string')

        return RawEvent(
            event_id=str(data['event_id']),
            event_type=str(data['event_type']),
            source=str(data['source']),
            payload=data.get('payload', {}),
            trace_context=data.get('trace_context', {}),
            identifiers=RawEventIdentifiers(
                anonymous_id=str(anonymous_id) if anonymous_id else None,
                known=tuple(known),
            ),
            occurred_at=datetime.fromisoformat(occurred_at_raw),
            received_at=now,
            created_at=datetime.fromisoformat(created_at_raw),
        )


class WorkerMessageRouter:
    def __init__(
        self,
        producer: AIOKafkaProducer,
        process_event_service: EventProcessingService,
        event_mapper: EventMapper,
        now_provider: Callable[[], datetime],
        events_v1_topic: str,
        events_dlq_topic: str,
    ) -> None:
        self._producer = producer
        self._process_event_service = process_event_service
        self._event_mapper = event_mapper
        self._now_provider = now_provider
        self._events_v1_topic = events_v1_topic
        self._events_dlq_topic = events_dlq_topic

    async def dispatch(self, message: Any) -> None:
        if message.topic == self._events_v1_topic:
            await self._handle_cdp_event(message)
            return

        raise UnknownWorkerTopicError(f'No handler for topic {message.topic}')

    async def send(
        self,
        topic_name: str,
        payload: dict[str, object],
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        value = json.dumps(payload).encode('utf-8')
        await self._producer.send_and_wait(
            topic_name, value=value, key=key, headers=headers
        )

    async def _handle_cdp_event(self, message: Any) -> None:
        raw_event = None
        try:
            raw_event = self._event_mapper.to_raw_event(message)
        except InvalidEventError as e:
            logger.error(
                'event_processing_failed reason=invalid_event_structure error="%s"', e
            )
            await self._send_to_dlq(
                message, reason='invalid_event_structure', error_message=str(e)
            )
            return
        except Exception as e:
            logger.error(
                'event_processing_failed reason=mapping_error error="%s"',
                e,
                exc_info=True,
            )
            await self._send_to_dlq(
                message, reason='mapping_error', error_message=str(e)
            )
            return

        logger.info(
            'raw_event_mapped event_id=%s event_type=%s source=%s',
            raw_event.event_id,
            raw_event.event_type,
            raw_event.source,
        )

        result = await self._process_event_service.process_event(raw_event)

        log_meta = f'event_id={raw_event.event_id} event_type={raw_event.event_type} source={raw_event.source}'

        if result.outcome == ProcessEventOutcome.PROCESSED:
            logger.info(
                'customer_profile_updated %s customer_id=%s processing_status=processed',
                log_meta,
                result.customer_id,
            )
            # In CDP, profile update implies segment recalculation intent
            logger.info(
                'segment_membership_updated %s customer_id=%s',
                log_meta,
                result.customer_id,
            )
            return

        if result.outcome == ProcessEventOutcome.IGNORED_ANONYMOUS:
            logger.info(
                'anonymous_event_ignored_for_profile %s processing_status=ignored_anonymous',
                log_meta,
            )
            return

        if result.outcome == ProcessEventOutcome.SEND_TO_DLQ:
            logger.warning(
                'event_sent_to_dlq %s reason=%s processing_status=sent_to_dlq',
                log_meta,
                result.reason,
            )
            await self._send_to_dlq(
                message,
                reason=result.reason or 'unknown_failure',
                event_id=raw_event.event_id,
                source=raw_event.source,
                trace_context=dict(raw_event.trace_context),
            )
            return

        if result.outcome == ProcessEventOutcome.FAILED:
            logger.error(
                'event_processing_failed %s reason=%s error_reason="%s"',
                log_meta,
                result.outcome,
                result.reason,
            )
            raise ProcessingRetryError(
                f'processing failed for event {result.event_id}: {result.reason}'
            )

    async def _send_to_dlq(
        self,
        message,
        reason: str,
        error_message: str | None = None,
        event_id: str | None = None,
        source: str | None = None,
        trace_context: dict[str, object] | None = None,
    ) -> None:
        raw_value = (
            message.value.decode('utf-8', errors='replace') if message.value else None
        )

        payload = {
            'original_event': raw_value,
            'reason': reason,
            'error_message': error_message,
            'failed_at': self._now_provider().isoformat(),
            'source': source,
            'event_id': event_id,
            'trace_context': trace_context or {},
            'metadata': {
                'kafka_topic': message.topic,
                'kafka_partition': message.partition,
                'kafka_offset': message.offset,
            },
        }
        try:
            await self.send(self._events_dlq_topic, payload)
        except Exception as e:
            logger.error('dlq_publish_failed reason=%s error="%s"', reason, e)
            raise ProcessingRetryError(f'failed to send to DLQ: {e}') from e
