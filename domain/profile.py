from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping

from domain.error import DomainError, UnsupportedCurrencyError
from domain.event import EventType
from domain.identity import CustomerIdentifiers
from domain.segment import SegmentId


class Currency(StrEnum):
    RUB = 'RUB'


@dataclass(frozen=True, slots=True)
class RecentEvent:
    event_id: str
    event_type: EventType
    occurred_at: datetime

    def __post_init__(self) -> None:
        _validate_non_empty(self.event_id, 'event_id')

        try:
            event_type = EventType(self.event_type)
        except ValueError as error:
            raise DomainError(
                f'unsupported recent event type: {self.event_type}'
            ) from error

        object.__setattr__(self, 'event_type', event_type)


@dataclass(frozen=True, slots=True)
class CustomerProfile:
    customer_id: str
    first_seen_at: datetime
    last_seen_at: datetime
    created_at: datetime
    updated_at: datetime
    identifiers: CustomerIdentifiers = field(default_factory=CustomerIdentifiers)
    attributes: Mapping[str, object] = field(default_factory=dict)
    last_purchase_at: datetime | None = None
    events_count: int = 0
    page_views_count: int = 0
    cart_adds_count: int = 0
    orders_count: int = 0
    total_revenue: Decimal = Decimal('0')
    currency: Currency = Currency.RUB
    current_segments: frozenset[SegmentId] = field(default_factory=frozenset)
    recent_events: tuple[RecentEvent, ...] = ()

    def __post_init__(self) -> None:
        _validate_non_empty(self.customer_id, 'customer_id')
        _validate_counter(self.events_count, 'events_count')
        _validate_counter(self.page_views_count, 'page_views_count')
        _validate_counter(self.cart_adds_count, 'cart_adds_count')
        _validate_counter(self.orders_count, 'orders_count')

        object.__setattr__(self, 'currency', _rub_currency(self.currency))
        object.__setattr__(self, 'attributes', MappingProxyType(dict(self.attributes)))
        object.__setattr__(
            self,
            'total_revenue',
            _revenue_value(self.total_revenue),
        )
        object.__setattr__(
            self,
            'current_segments',
            frozenset(SegmentId(segment_id) for segment_id in self.current_segments),
        )
        recent_events = tuple(self.recent_events)
        for recent_event in recent_events:
            if not isinstance(recent_event, RecentEvent):
                raise DomainError('recent_events must contain only RecentEvent values')

        object.__setattr__(self, 'recent_events', recent_events)

    @property
    def primary_email(self) -> str | None:
        return self.identifiers.emails[0] if self.identifiers.emails else None

    @property
    def primary_phone(self) -> str | None:
        return self.identifiers.phones[0] if self.identifiers.phones else None

    @property
    def primary_external_user_id(self) -> str | None:
        if not self.identifiers.external_user_ids:
            return None
        return self.identifiers.external_user_ids[0]


def _rub_currency(currency: Currency) -> Currency:
    try:
        value = Currency(currency)
    except ValueError as error:
        raise UnsupportedCurrencyError(f'unsupported currency: {currency}') from error

    if value != Currency.RUB:
        raise UnsupportedCurrencyError(f'unsupported currency: {currency}')

    return value


def _revenue_value(value: Decimal) -> Decimal:
    try:
        revenue = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise DomainError(f'total_revenue must be a valid Decimal: {value}') from error

    if not revenue.is_finite():
        raise DomainError(f'total_revenue must be finite: {value}')

    if revenue < Decimal('0'):
        raise DomainError('total_revenue must not be negative')

    return revenue


def _validate_counter(value: int, field_name: str) -> None:
    if value < 0:
        raise DomainError(f'{field_name} must not be negative')


def _validate_non_empty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise DomainError(f'{field_name} must be non-empty')
