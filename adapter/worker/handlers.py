import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from aiokafka import AIOKafkaProducer  # type: ignore[import-untyped]

from domain.error import InvalidEventError
from domain.event import RawEvent, RawEventIdentifiers
from domain.identity import KnownIdentifier, KnownIdentifierType
from usecase.dto import ProcessEventOutcome
from usecase.interface import Logger, Metrics, Tracer
from usecase.process_event import EventProcessingService


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
            raise ValueError(f'invalid json: {e}') from e

        if not isinstance(data, dict):
            raise InvalidEventError('event must be a json object')

        for field in ['event_id', 'source', 'occurred_at']:
            if field not in data:
                raise InvalidEventError(f'missing required field: {field}')

        identifiers_raw = data.get(
            'identifiers', {'anonymous_id': str(data['event_id'])}
        )
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
            event_type=str(data.get('event_type', 'page_view')),
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
        logger: Logger,
        tracer: Tracer,
        metrics: Metrics,
    ) -> None:
        self._producer = producer
        self._process_event_service = process_event_service
        self._event_mapper = event_mapper
        self._now_provider = now_provider
        self._events_v1_topic = events_v1_topic
        self._events_dlq_topic = events_dlq_topic
        self._logger = logger
        self._tracer = tracer
        self._metrics = metrics

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
        with self._tracer.start_span('worker.handle_cdp_event') as span:
            raw_event = None
            try:
                raw_event = self._event_mapper.to_raw_event(message)
            except InvalidEventError as e:
                self._logger.error(
                    'event processing failed',
                    attrs={'reason': 'invalid_event_structure', 'error': str(e)},
                )
                span.record_error(e)
                span.set_attribute('outcome', 'invalid_event')
                self._metrics.increment(
                    'events_dlq_total',
                    attrs={'reason': 'invalid_event_structure'},
                )
                await self._send_to_dlq(
                    message,
                    reason='invalid_event_structure',
                    error_message=str(e),
                )
                return
            except Exception as e:
                self._logger.error(
                    'event processing failed',
                    attrs={'reason': 'mapping_error', 'error': str(e)},
                )
                span.record_error(e)
                span.set_attribute('outcome', 'mapping_error')
                self._metrics.increment(
                    'events_dlq_total',
                    attrs={'reason': 'invalid_event_structure'},
                )
                await self._send_to_dlq(
                    message,
                    reason='invalid_event_structure',
                    error_message=str(e),
                )
                return

            span.set_attribute('event_id', raw_event.event_id)
            span.set_attribute('event_type', raw_event.event_type)
            self._logger.info(
                'raw event mapped',
                attrs={
                    'event_id': raw_event.event_id,
                    'event_type': raw_event.event_type,
                    'source': raw_event.source,
                },
            )

            result = await self._process_event_service.process_event(raw_event)

            if result.outcome == ProcessEventOutcome.PROCESSED:
                self._logger.info(
                    'event processed',
                    attrs={
                        'event_id': raw_event.event_id,
                        'customer_id': result.customer_id or '',
                    },
                )
                span.set_attribute('outcome', 'processed')
                self._metrics.increment(
                    'events_processed_total',
                    attrs={
                        'event_type': raw_event.event_type,
                        'outcome': 'processed',
                    },
                )
                return

            if result.outcome == ProcessEventOutcome.IGNORED_ANONYMOUS:
                self._logger.info(
                    'anonymous event ignored',
                    attrs={'event_id': raw_event.event_id},
                )
                span.set_attribute('outcome', 'ignored_anonymous')
                self._metrics.increment(
                    'events_processed_total',
                    attrs={
                        'event_type': raw_event.event_type,
                        'outcome': 'ignored_anonymous',
                    },
                )
                return

            if result.outcome == ProcessEventOutcome.SEND_TO_DLQ:
                self._logger.warning(
                    'event sent to dlq',
                    attrs={
                        'event_id': raw_event.event_id,
                        'reason': result.reason or 'unknown',
                    },
                )
                span.set_attribute('outcome', 'sent_to_dlq')
                self._metrics.increment(
                    'events_dlq_total',
                    attrs={'reason': result.reason or 'unknown'},
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
                self._logger.error(
                    'event processing failed',
                    attrs={
                        'event_id': raw_event.event_id,
                        'reason': result.reason or 'unknown',
                    },
                )
                span.set_attribute('outcome', 'failed')
                self._metrics.increment(
                    'events_failed_total',
                    attrs={'reason': result.reason or 'unknown'},
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
                'kafka_topic': _message_topic(message),
                'kafka_partition': _message_int_attr(message, 'partition'),
                'kafka_offset': _message_int_attr(message, 'offset'),
            },
        }
        try:
            await self.send(self._events_dlq_topic, payload)
        except Exception as e:
            self._logger.error(
                'dlq publish failed',
                attrs={'reason': reason, 'error': str(e)},
            )
            raise ProcessingRetryError(f'failed to send to DLQ: {e}') from e


def _message_topic(message: Any) -> str | None:
    topic = getattr(message, 'topic', None)
    if isinstance(topic, str):
        return topic
    return None


def _message_int_attr(message: Any, name: str) -> int | None:
    value = getattr(message, name, None)
    if isinstance(value, int):
        return value
    return None
