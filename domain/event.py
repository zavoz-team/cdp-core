from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping

from domain.error import InvalidEventError
from domain.identity import KnownIdentifier


class EventType(StrEnum):
    PAGE_VIEW = 'page_view'
    ADD_TO_CART = 'add_to_cart'
    PURCHASE = 'purchase'


class RawEventProcessingStatus(StrEnum):
    RECEIVED = 'received'
    PROCESSED = 'processed'
    IGNORED_ANONYMOUS = 'ignored_anonymous'
    FAILED = 'failed'
    SENT_TO_DLQ = 'sent_to_dlq'


@dataclass(frozen=True, slots=True)
class RawEventIdentifiers:
    known: tuple[KnownIdentifier, ...] = ()
    anonymous_id: str | None = None

    def __post_init__(self) -> None:
        known = tuple(self.known)
        for identifier in known:
            if not isinstance(identifier, KnownIdentifier):
                raise InvalidEventError(
                    'known raw identifiers must be KnownIdentifier values'
                )

        object.__setattr__(self, 'known', known)

        if self.anonymous_id is not None and (
            not isinstance(self.anonymous_id, str) or not self.anonymous_id.strip()
        ):
            raise InvalidEventError('anonymous_id must be non-empty when provided')

        if not known and self.anonymous_id is None:
            raise InvalidEventError(
                'raw event identifiers must include known identifier or anonymous_id'
            )

    @property
    def has_known(self) -> bool:
        return bool(self.known)

    @property
    def is_anonymous_only(self) -> bool:
        return self.anonymous_id is not None and not self.known


@dataclass(frozen=True, slots=True)
class RawEvent:
    event_id: str
    event_type: EventType | str
    source: str
    occurred_at: datetime
    received_at: datetime
    created_at: datetime
    identifiers: RawEventIdentifiers = field(default_factory=RawEventIdentifiers)
    attributes: Mapping[str, object] = field(default_factory=dict)
    payload: Mapping[str, object] = field(default_factory=dict)
    trace_context: Mapping[str, object] = field(default_factory=dict)
    processing_status: RawEventProcessingStatus = RawEventProcessingStatus.RECEIVED
    error_reason: str | None = None

    def __post_init__(self) -> None:
        _validate_non_empty(self.event_id, 'event_id')
        _validate_non_empty(self.source, 'source')

        object.__setattr__(self, 'event_type', _event_type_value(self.event_type))
        object.__setattr__(
            self,
            'processing_status',
            _processing_status_value(self.processing_status),
        )
        object.__setattr__(self, 'attributes', _frozen_mapping(self.attributes))
        object.__setattr__(self, 'payload', _frozen_mapping(self.payload))
        object.__setattr__(self, 'trace_context', _frozen_mapping(self.trace_context))

    def supported_event_type(self) -> EventType | None:
        if isinstance(self.event_type, EventType):
            return self.event_type
        return None


def _event_type_value(event_type: EventType | str) -> EventType | str:
    if not isinstance(event_type, str) or not event_type.strip():
        raise InvalidEventError('event_type must be non-empty')

    try:
        return EventType(event_type)
    except ValueError:
        return event_type


def _processing_status_value(
    status: RawEventProcessingStatus,
) -> RawEventProcessingStatus:
    try:
        return RawEventProcessingStatus(status)
    except ValueError as error:
        raise InvalidEventError(
            f'unsupported raw event processing status: {status}'
        ) from error


def _frozen_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType(dict(value))


def _validate_non_empty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise InvalidEventError(f'{field_name} must be non-empty')
