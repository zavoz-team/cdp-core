import json
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from domain.event import RawEvent, RawEventIdentifiers, RawEventProcessingStatus
from domain.identity import KnownIdentifier, KnownIdentifierType
from usecase.dto import RawEventRecordOutcome, RawEventRecordResult
from usecase.error import UseCaseDependencyError


class RawEventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record_received(self, raw_event: RawEvent) -> RawEventRecordResult:
        try:
            result = await self._session.execute(
                sa.text(
                    """
                    INSERT INTO raw_events (
                        event_id, event_type, source,
                        occurred_at, received_at, created_at,
                        identifiers_json, attributes_json, payload_json, trace_context_json,
                        processing_status, error_reason
                    ) VALUES (
                        :event_id, :event_type, :source,
                        :occurred_at, :received_at, :created_at,
                        CAST(:identifiers_json AS jsonb), CAST(:attributes_json AS jsonb),
                        CAST(:payload_json AS jsonb), CAST(:trace_context_json AS jsonb),
                        :processing_status, :error_reason
                    )
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING event_id
                    """
                ),
                _insert_params(raw_event),
            )
        except Exception as exc:
            raise UseCaseDependencyError('raw event record failed') from exc

        if result.first() is None:
            return RawEventRecordResult(
                outcome=RawEventRecordOutcome.DUPLICATE,
                event_id=raw_event.event_id,
            )
        return RawEventRecordResult(
            outcome=RawEventRecordOutcome.CREATED,
            event_id=raw_event.event_id,
        )

    async def get_by_event_id(self, event_id: str) -> RawEvent | None:
        try:
            result = await self._session.execute(
                sa.text(
                    """
                    SELECT
                        event_id, event_type, source,
                        occurred_at, received_at, created_at,
                        identifiers_json, attributes_json, payload_json, trace_context_json,
                        processing_status, error_reason
                    FROM raw_events
                    WHERE event_id = :event_id
                    """
                ),
                {'event_id': event_id},
            )
        except Exception as exc:
            raise UseCaseDependencyError('raw event lookup failed') from exc

        row = result.mappings().first()
        if row is None:
            return None
        return _row_to_event(row)

    async def mark_processed(self, event_id: str) -> None:
        await self._update_status(event_id, RawEventProcessingStatus.PROCESSED)

    async def mark_ignored_anonymous(self, event_id: str) -> None:
        await self._update_status(event_id, RawEventProcessingStatus.IGNORED_ANONYMOUS)

    async def mark_sent_to_dlq(self, event_id: str, reason: str) -> None:
        await self._update_status(
            event_id, RawEventProcessingStatus.SENT_TO_DLQ, reason
        )

    async def mark_failed(self, event_id: str, reason: str) -> None:
        await self._update_status(event_id, RawEventProcessingStatus.FAILED, reason)

    async def _update_status(
        self,
        event_id: str,
        status: RawEventProcessingStatus,
        error_reason: str | None = None,
    ) -> None:
        try:
            await self._session.execute(
                sa.text(
                    """
                    UPDATE raw_events
                    SET processing_status = :status,
                        error_reason      = :error_reason
                    WHERE event_id = :event_id
                    """
                ),
                {
                    'event_id': event_id,
                    'status': status.value,
                    'error_reason': error_reason,
                },
            )
        except Exception as exc:
            raise UseCaseDependencyError('raw event status update failed') from exc


def _insert_params(event: RawEvent) -> dict[str, Any]:
    return {
        'event_id': event.event_id,
        'event_type': event.event_type.value
        if hasattr(event.event_type, 'value')
        else str(event.event_type),
        'source': event.source,
        'occurred_at': event.occurred_at,
        'received_at': event.received_at,
        'created_at': event.created_at,
        'identifiers_json': json.dumps(_identifiers_to_dict(event.identifiers)),
        'attributes_json': json.dumps(dict(event.attributes)),
        'payload_json': json.dumps(dict(event.payload)),
        'trace_context_json': json.dumps(dict(event.trace_context)),
        'processing_status': RawEventProcessingStatus.RECEIVED.value,
        'error_reason': None,
    }


def _identifiers_to_dict(identifiers: RawEventIdentifiers) -> dict[str, Any]:
    return {
        'known': [
            {'identifier_type': k.identifier_type.value, 'value': k.value}
            for k in identifiers.known
        ],
        'anonymous_id': identifiers.anonymous_id,
    }


def _dict_to_identifiers(data: dict[str, Any]) -> RawEventIdentifiers:
    known = tuple(
        KnownIdentifier(
            identifier_type=KnownIdentifierType(item['identifier_type']),
            value=item['value'],
        )
        for item in data.get('known', [])
    )
    return RawEventIdentifiers(
        known=known,
        anonymous_id=data.get('anonymous_id'),
    )


def _parse_json(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _row_to_event(row: Any) -> RawEvent:
    return RawEvent(
        event_id=row['event_id'],
        event_type=row['event_type'],
        source=row['source'],
        occurred_at=row['occurred_at'],
        received_at=row['received_at'],
        created_at=row['created_at'],
        identifiers=_dict_to_identifiers(_parse_json(row['identifiers_json'])),
        attributes=_parse_json(row['attributes_json']),
        payload=_parse_json(row['payload_json']),
        trace_context=_parse_json(row['trace_context_json']),
        processing_status=row['processing_status'],
        error_reason=row['error_reason'],
    )
