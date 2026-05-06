from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Mapping

from domain.error import DomainError, UnsupportedCurrencyError
from domain.event import EventType, RawEvent
from domain.profile import Currency, CustomerProfile, RecentEvent
from domain.segment import SegmentId

NEW_USER_WINDOW = timedelta(days=7)
ACTIVE_USER_WINDOW = timedelta(days=30)
VIP_REVENUE_THRESHOLD = Decimal('100000')


@dataclass(frozen=True, slots=True)
class PurchaseFacts:
    order_id: str
    amount: Decimal
    currency: Currency


def update_profile_from_event(
    profile: CustomerProfile,
    event: RawEvent,
) -> CustomerProfile:
    event_type = event.supported_event_type()
    if event_type is None or not event.identifiers.has_known:
        return profile

    amount = Decimal('0')
    last_purchase_at = profile.last_purchase_at
    orders_count = profile.orders_count
    if event_type == EventType.PURCHASE:
        purchase = validate_purchase_payload(event.payload)
        amount = purchase.amount
        last_purchase_at = _latest_datetime(profile.last_purchase_at, event.occurred_at)
        orders_count += 1

    return replace(
        profile,
        first_seen_at=min(profile.first_seen_at, event.occurred_at),
        last_seen_at=max(profile.last_seen_at, event.occurred_at),
        last_purchase_at=last_purchase_at,
        events_count=profile.events_count + 1,
        page_views_count=profile.page_views_count
        + (1 if event_type == EventType.PAGE_VIEW else 0),
        cart_adds_count=profile.cart_adds_count
        + (1 if event_type == EventType.ADD_TO_CART else 0),
        orders_count=orders_count,
        total_revenue=profile.total_revenue + amount,
        recent_events=profile.recent_events
        + (
            RecentEvent(
                event_id=event.event_id,
                event_type=event_type,
                occurred_at=event.occurred_at,
            ),
        ),
    )


def evaluate_static_segments(
    profile: CustomerProfile,
    now: datetime,
) -> frozenset[SegmentId]:
    segments: set[SegmentId] = set()

    if profile.orders_count == 0 and _within_last(
        profile.first_seen_at,
        now,
        NEW_USER_WINDOW,
    ):
        segments.add(SegmentId.NEW_USER)

    if profile.events_count >= 3 and _within_last(
        profile.last_seen_at,
        now,
        ACTIVE_USER_WINDOW,
    ):
        segments.add(SegmentId.ACTIVE)

    if profile.total_revenue >= VIP_REVENUE_THRESHOLD or profile.orders_count >= 3:
        segments.add(SegmentId.VIP)

    return frozenset(segments)


def update_profile_segments(
    profile: CustomerProfile,
    now: datetime,
) -> CustomerProfile:
    return replace(profile, current_segments=evaluate_static_segments(profile, now))


def validate_purchase_payload(payload: Mapping[str, object]) -> PurchaseFacts:
    return PurchaseFacts(
        order_id=_purchase_order_id(payload),
        amount=_purchase_amount(payload),
        currency=_purchase_currency(payload),
    )


def _purchase_amount(payload: Mapping[str, object]) -> Decimal:
    amount = payload.get('amount')
    if amount is None:
        raise DomainError('purchase amount is required')

    if isinstance(amount, bool):
        raise DomainError(f'purchase amount must be a valid Decimal: {amount}')

    if isinstance(amount, Decimal):
        return _valid_purchase_amount(amount)

    if isinstance(amount, int):
        return _valid_purchase_amount(Decimal(amount))

    if isinstance(amount, str):
        try:
            return _valid_purchase_amount(Decimal(amount))
        except (InvalidOperation, ValueError) as error:
            raise DomainError(
                f'purchase amount must be a valid Decimal: {amount}'
            ) from error

    raise DomainError(f'purchase amount must be a valid Decimal: {amount}')


def _valid_purchase_amount(amount: Decimal) -> Decimal:
    if not amount.is_finite():
        raise DomainError(f'purchase amount must be finite: {amount}')

    if amount < Decimal('0'):
        raise DomainError('purchase amount must not be negative')

    return amount


def _purchase_currency(payload: Mapping[str, object]) -> Currency:
    currency = payload.get('currency')
    if not isinstance(currency, str):
        raise UnsupportedCurrencyError('purchase currency must be RUB')

    try:
        value = Currency(currency)
    except ValueError as error:
        raise UnsupportedCurrencyError('purchase currency must be RUB') from error

    if value != Currency.RUB:
        raise UnsupportedCurrencyError('purchase currency must be RUB')

    return value


def _purchase_order_id(payload: Mapping[str, object]) -> str:
    order_id = payload.get('order_id')
    if not isinstance(order_id, str) or not order_id.strip():
        raise DomainError('purchase order_id is required')

    return order_id


def _latest_datetime(current: datetime | None, candidate: datetime) -> datetime:
    if current is None or candidate > current:
        return candidate
    return current


def _within_last(value: datetime, now: datetime, window: timedelta) -> bool:
    return now - window <= value <= now
