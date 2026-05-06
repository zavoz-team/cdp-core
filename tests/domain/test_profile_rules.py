from datetime import datetime
from decimal import Decimal

import pytest

from domain.error import UnsupportedCurrencyError
from domain.event import EventType, RawEvent, RawEventIdentifiers
from domain.identity import CustomerIdentifiers, KnownIdentifier, KnownIdentifierType
from domain.profile import CustomerProfile
from domain.rules import update_profile_from_event, update_profile_segments
from domain.segment import SegmentId

NOW = datetime(2026, 1, 1, 12, 0, 0)


def profile(customer_id: str = 'customer-1') -> CustomerProfile:
    return CustomerProfile(
        customer_id=customer_id,
        first_seen_at=NOW,
        last_seen_at=NOW,
        created_at=NOW,
        updated_at=NOW,
        identifiers=CustomerIdentifiers(emails=('alice@example.com',)),
    )


def raw_event(
    event_id: str,
    event_type: EventType,
    identifiers: RawEventIdentifiers,
    payload: dict[str, object] | None = None,
) -> RawEvent:
    return RawEvent(
        event_id=event_id,
        event_type=event_type,
        source='web',
        occurred_at=NOW,
        received_at=NOW,
        created_at=NOW,
        identifiers=identifiers,
        payload=payload or {},
    )


def known_identifiers() -> RawEventIdentifiers:
    return RawEventIdentifiers(
        known=(KnownIdentifier(KnownIdentifierType.EMAIL, 'alice@example.com'),)
    )


def test_known_page_view_updates_profile_counts_and_recent_events() -> None:
    updated = update_profile_from_event(
        profile(),
        raw_event('event-1', EventType.PAGE_VIEW, known_identifiers()),
    )

    assert updated.events_count == 1
    assert updated.page_views_count == 1
    assert updated.orders_count == 0
    assert updated.total_revenue == Decimal('0')
    assert updated.recent_events[0].event_id == 'event-1'
    assert updated.recent_events[0].event_type == EventType.PAGE_VIEW


def test_anonymous_page_view_does_not_change_profile() -> None:
    existing = profile()

    updated = update_profile_from_event(
        existing,
        raw_event(
            'event-2',
            EventType.PAGE_VIEW,
            RawEventIdentifiers(anonymous_id='anonymous-1'),
        ),
    )

    assert updated == existing


def test_known_purchase_in_rub_updates_order_facts() -> None:
    updated = update_profile_from_event(
        profile(),
        raw_event(
            'event-3',
            EventType.PURCHASE,
            known_identifiers(),
            {'order_id': 'order-1', 'amount': '1250.50', 'currency': 'RUB'},
        ),
    )

    assert updated.events_count == 1
    assert updated.orders_count == 1
    assert updated.total_revenue == Decimal('1250.50')
    assert updated.last_purchase_at == NOW


def test_unsupported_purchase_currency_is_rejected() -> None:
    with pytest.raises(UnsupportedCurrencyError):
        update_profile_from_event(
            profile(),
            raw_event(
                'event-4',
                EventType.PURCHASE,
                known_identifiers(),
                {'order_id': 'order-1', 'amount': '100', 'currency': 'USD'},
            ),
        )


def test_segment_membership_is_derived_from_updated_profile() -> None:
    active_profile = CustomerProfile(
        customer_id='customer-1',
        first_seen_at=NOW,
        last_seen_at=NOW,
        created_at=NOW,
        updated_at=NOW,
        identifiers=CustomerIdentifiers(emails=('alice@example.com',)),
        events_count=3,
    )

    updated = update_profile_segments(active_profile, NOW)

    assert updated.current_segments == frozenset({SegmentId.NEW_USER, SegmentId.ACTIVE})
