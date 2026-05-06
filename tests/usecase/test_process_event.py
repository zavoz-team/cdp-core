import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

import pytest

from domain.error import DomainError, UnsupportedCurrencyError
from domain.event import (
    EventType,
    RawEvent,
    RawEventIdentifiers,
    RawEventProcessingStatus,
)
from domain.identity import (
    CustomerIdentifiers,
    IdentityLink,
    KnownIdentifier,
    KnownIdentifierType,
)
from domain.profile import Currency, CustomerProfile
from domain.segment import SegmentDefinition, SegmentId, SegmentMembership
from usecase import process_event as process_event_module
from usecase.criteria import ProfileListCriteria
from usecase.dto import (
    ProcessedPurchase,
    ProcessEventOutcome,
    PurchaseRecordOutcome,
    PurchaseRecordResult,
    RawEventRecordOutcome,
    RawEventRecordResult,
)
from usecase.error import UseCaseDependencyError
from usecase.interface import (
    CustomerProfileRepository,
    EventProcessingTransaction,
    IdentityRepository,
    PurchaseRepository,
    RawEventRepository,
    SegmentRepository,
)
from usecase.process_event import (
    REASON_IDENTITY_CONFLICT,
    REASON_INVALID_GENERATED_CUSTOMER_ID,
    REASON_INVALID_PURCHASE_PAYLOAD,
    REASON_LINKED_PROFILE_NOT_FOUND,
    REASON_PROCESSING_ERROR,
    REASON_PURCHASE_CONFLICT,
    REASON_UNSUPPORTED_CURRENCY,
    REASON_UNSUPPORTED_EVENT_TYPE,
    EventProcessingService,
)

NOW = datetime(2026, 1, 1, 12, 0, 0)


@dataclass
class FakeEventProcessingState:
    raw_events: dict[str, RawEvent] = field(default_factory=dict)
    raw_statuses: dict[str, RawEventProcessingStatus] = field(default_factory=dict)
    raw_reasons: dict[str, str] = field(default_factory=dict)
    profiles: dict[str, CustomerProfile] = field(default_factory=dict)
    profile_saves: list[CustomerProfile] = field(default_factory=list)
    links: dict[tuple[KnownIdentifierType, str], IdentityLink] = field(
        default_factory=dict
    )
    saved_links: list[IdentityLink] = field(default_factory=list)
    purchases: dict[tuple[str, str], ProcessedPurchase] = field(default_factory=dict)
    purchase_records: list[ProcessedPurchase] = field(default_factory=list)
    memberships: dict[str, tuple[SegmentMembership, ...]] = field(default_factory=dict)
    membership_replacements: list[tuple[str, tuple[SegmentMembership, ...]]] = field(
        default_factory=list
    )
    commits: int = 0
    rollbacks: int = 0
    begin_calls: int = 0
    begin_errors_remaining: int = 0
    begin_error_on_call: int | None = None
    record_errors_remaining: int = 0
    commit_errors_remaining: int = 0
    rollback_errors_remaining: int = 0


class FakeRawEventRepository:
    def __init__(self, state: FakeEventProcessingState) -> None:
        self._state = state

    async def get_by_event_id(self, event_id: str) -> RawEvent | None:
        return self._state.raw_events.get(event_id)

    async def record_received(self, raw_event: RawEvent) -> RawEventRecordResult:
        if self._state.record_errors_remaining > 0:
            self._state.record_errors_remaining -= 1
            raise UseCaseDependencyError('raw event repository unavailable')

        if raw_event.event_id in self._state.raw_events:
            return RawEventRecordResult(
                RawEventRecordOutcome.DUPLICATE,
                raw_event.event_id,
            )

        self._state.raw_events[raw_event.event_id] = raw_event
        self._state.raw_statuses[raw_event.event_id] = RawEventProcessingStatus.RECEIVED
        return RawEventRecordResult(RawEventRecordOutcome.CREATED, raw_event.event_id)

    async def mark_processed(self, event_id: str) -> None:
        self._state.raw_statuses[event_id] = RawEventProcessingStatus.PROCESSED

    async def mark_ignored_anonymous(self, event_id: str) -> None:
        self._state.raw_statuses[event_id] = RawEventProcessingStatus.IGNORED_ANONYMOUS

    async def mark_sent_to_dlq(self, event_id: str, reason: str) -> None:
        self._state.raw_statuses[event_id] = RawEventProcessingStatus.SENT_TO_DLQ
        self._state.raw_reasons[event_id] = reason

    async def mark_failed(self, event_id: str, reason: str) -> None:
        self._state.raw_statuses[event_id] = RawEventProcessingStatus.FAILED
        self._state.raw_reasons[event_id] = reason


class FakeCustomerProfileRepository:
    def __init__(self, state: FakeEventProcessingState) -> None:
        self._state = state

    async def get_by_customer_id(self, customer_id: str) -> CustomerProfile | None:
        return self._state.profiles.get(customer_id)

    async def get_many_by_customer_ids(
        self,
        customer_ids: tuple[str, ...],
    ) -> tuple[CustomerProfile, ...]:
        return tuple(
            profile
            for customer_id in customer_ids
            if (profile := self._state.profiles.get(customer_id)) is not None
        )

    async def list_profiles(
        self,
        criteria: ProfileListCriteria,
    ) -> tuple[CustomerProfile, ...]:
        return tuple(self._state.profiles.values())[
            criteria.offset : criteria.offset + criteria.limit
        ]

    async def count_profiles(self, criteria: ProfileListCriteria) -> int:
        return len(tuple(self._state.profiles.values())[criteria.offset :])

    async def save_profile(self, profile: CustomerProfile) -> None:
        self._state.profiles[profile.customer_id] = profile
        self._state.profile_saves.append(profile)


class FakeIdentityRepository:
    def __init__(self, state: FakeEventProcessingState) -> None:
        self._state = state

    async def get_link(self, identifier: KnownIdentifier) -> IdentityLink | None:
        return self._state.links.get((identifier.identifier_type, identifier.value))

    async def find_links(
        self,
        identifiers: tuple[KnownIdentifier, ...],
    ) -> tuple[IdentityLink, ...]:
        links: list[IdentityLink] = []
        for identifier in identifiers:
            link = await self.get_link(identifier)
            if link is not None:
                links.append(link)

        return tuple(links)

    async def save_links(self, links: tuple[IdentityLink, ...]) -> None:
        for link in links:
            self._state.links[(link.identity_type, link.identity_value)] = link
            self._state.saved_links.append(link)


class FakePurchaseRepository:
    def __init__(self, state: FakeEventProcessingState) -> None:
        self._state = state

    async def get_by_source_order_id(
        self,
        source: str,
        order_id: str,
    ) -> ProcessedPurchase | None:
        return self._state.purchases.get((source, order_id))

    async def record_processed_purchase(
        self,
        purchase: ProcessedPurchase,
    ) -> PurchaseRecordResult:
        key = (purchase.source, purchase.order_id)
        existing = self._state.purchases.get(key)
        if existing is None:
            self._state.purchases[key] = purchase
            self._state.purchase_records.append(purchase)
            return PurchaseRecordResult(
                PurchaseRecordOutcome.RECORDED,
                purchase.source,
                purchase.order_id,
            )

        if (
            existing.amount == purchase.amount
            and existing.currency == purchase.currency
        ):
            return PurchaseRecordResult(
                PurchaseRecordOutcome.DUPLICATE,
                purchase.source,
                purchase.order_id,
                existing,
            )

        return PurchaseRecordResult(
            PurchaseRecordOutcome.CONFLICT,
            purchase.source,
            purchase.order_id,
            existing,
        )


class FakeSegmentRepository:
    def __init__(self, state: FakeEventProcessingState) -> None:
        self._state = state

    async def get_definition(self, segment_id: SegmentId) -> SegmentDefinition | None:
        return SegmentDefinition(segment_id, str(segment_id), str(segment_id))

    async def list_definitions(
        self,
        include_disabled: bool = False,
    ) -> tuple[SegmentDefinition, ...]:
        return (
            SegmentDefinition(SegmentId.NEW_USER, 'New User', 'New User'),
            SegmentDefinition(SegmentId.ACTIVE, 'Active', 'Active'),
            SegmentDefinition(SegmentId.VIP, 'VIP', 'VIP'),
        )

    async def count_members(self, segment_id: SegmentId) -> int:
        return sum(
            1
            for memberships in self._state.memberships.values()
            for membership in memberships
            if membership.segment_id == segment_id
        )

    async def list_memberships(
        self,
        segment_id: SegmentId,
        limit: int,
        offset: int,
    ) -> tuple[SegmentMembership, ...]:
        memberships = tuple(
            membership
            for profile_memberships in self._state.memberships.values()
            for membership in profile_memberships
            if membership.segment_id == segment_id
        )
        return memberships[offset : offset + limit]

    async def list_member_profiles(
        self,
        segment_id: SegmentId,
        limit: int,
        offset: int,
    ) -> tuple[CustomerProfile, ...]:
        members = tuple(
            profile
            for customer_id, memberships in self._state.memberships.items()
            if any(membership.segment_id == segment_id for membership in memberships)
            and (profile := self._state.profiles.get(customer_id)) is not None
        )
        return members[offset : offset + limit]

    async def replace_profile_memberships(
        self,
        customer_id: str,
        memberships: tuple[SegmentMembership, ...],
    ) -> None:
        self._state.memberships[customer_id] = memberships
        self._state.membership_replacements.append((customer_id, memberships))


class FakeEventProcessingTransaction:
    raw_events: RawEventRepository
    customer_profiles: CustomerProfileRepository
    identities: IdentityRepository
    purchases: PurchaseRepository
    segments: SegmentRepository

    def __init__(self, state: FakeEventProcessingState) -> None:
        self._state = state
        self.raw_events = FakeRawEventRepository(state)
        self.customer_profiles = FakeCustomerProfileRepository(state)
        self.identities = FakeIdentityRepository(state)
        self.purchases = FakePurchaseRepository(state)
        self.segments = FakeSegmentRepository(state)

    async def commit(self) -> None:
        if self._state.commit_errors_remaining > 0:
            self._state.commit_errors_remaining -= 1
            raise UseCaseDependencyError('commit unavailable')

        self._state.commits += 1

    async def rollback(self) -> None:
        if self._state.rollback_errors_remaining > 0:
            self._state.rollback_errors_remaining -= 1
            raise UseCaseDependencyError('rollback unavailable')

        self._state.rollbacks += 1


class FakeEventProcessingUnitOfWork:
    def __init__(self, state: FakeEventProcessingState) -> None:
        self._state = state

    async def begin(self) -> EventProcessingTransaction:
        self._state.begin_calls += 1
        if self._state.begin_calls == self._state.begin_error_on_call:
            raise UseCaseDependencyError('unit of work unavailable')

        if self._state.begin_errors_remaining > 0:
            self._state.begin_errors_remaining -= 1
            raise UseCaseDependencyError('unit of work unavailable')

        return FakeEventProcessingTransaction(self._state)


def service(state: FakeEventProcessingState) -> EventProcessingService:
    return EventProcessingService(
        FakeEventProcessingUnitOfWork(state),
        customer_id_generator=lambda: 'customer-1',
        now_provider=lambda: NOW,
    )


def run_event(state: FakeEventProcessingState, event: RawEvent):
    return asyncio.run(service(state).process_event(event))


def known_event(
    event_id: str,
    event_type: EventType | str = EventType.PAGE_VIEW,
    identifiers: tuple[KnownIdentifier, ...] | None = None,
    payload: dict[str, object] | None = None,
    source: str = 'web',
) -> RawEvent:
    return RawEvent(
        event_id=event_id,
        event_type=event_type,
        source=source,
        occurred_at=NOW,
        received_at=NOW,
        created_at=NOW,
        identifiers=RawEventIdentifiers(
            known=identifiers
            or (KnownIdentifier(KnownIdentifierType.EMAIL, 'alice@example.com'),)
        ),
        payload=payload or {},
    )


def anonymous_page_view(event_id: str) -> RawEvent:
    return RawEvent(
        event_id=event_id,
        event_type=EventType.PAGE_VIEW,
        source='web',
        occurred_at=NOW,
        received_at=NOW,
        created_at=NOW,
        identifiers=RawEventIdentifiers(anonymous_id='anonymous-1'),
    )


def existing_profile(
    customer_id: str = 'customer-1',
    orders_count: int = 0,
    total_revenue: Decimal = Decimal('0'),
) -> CustomerProfile:
    return CustomerProfile(
        customer_id=customer_id,
        first_seen_at=NOW,
        last_seen_at=NOW,
        created_at=NOW,
        updated_at=NOW,
        identifiers=CustomerIdentifiers(emails=('alice@example.com',)),
        events_count=1,
        orders_count=orders_count,
        total_revenue=total_revenue,
    )


def link_email_to_customer(
    state: FakeEventProcessingState,
    customer_id: str = 'customer-1',
) -> None:
    state.links[(KnownIdentifierType.EMAIL, 'alice@example.com')] = IdentityLink(
        KnownIdentifierType.EMAIL,
        'alice@example.com',
        customer_id,
    )


def test_known_page_view_creates_profile() -> None:
    state = FakeEventProcessingState()

    result = run_event(state, known_event('event-1'))

    saved = state.profiles['customer-1']
    assert result.outcome == ProcessEventOutcome.PROCESSED
    assert result.customer_id == 'customer-1'
    assert state.raw_statuses['event-1'] == RawEventProcessingStatus.PROCESSED
    assert saved.events_count == 1
    assert saved.page_views_count == 1
    assert saved.identifiers.emails == ('alice@example.com',)
    assert state.saved_links[0].customer_id == 'customer-1'


def test_anonymous_page_view_only_saves_raw_event() -> None:
    state = FakeEventProcessingState()

    result = run_event(state, anonymous_page_view('event-2'))

    assert result.outcome == ProcessEventOutcome.IGNORED_ANONYMOUS
    assert state.raw_events['event-2'].event_id == 'event-2'
    assert state.raw_statuses['event-2'] == RawEventProcessingStatus.IGNORED_ANONYMOUS
    assert state.profiles == {}
    assert state.saved_links == []
    assert state.purchase_records == []
    assert state.membership_replacements == []


def test_duplicate_event_id_does_not_update_profile_twice() -> None:
    state = FakeEventProcessingState()
    event = known_event('event-3')

    first = run_event(state, event)
    second = run_event(state, event)

    assert first.outcome == ProcessEventOutcome.PROCESSED
    assert second.outcome == ProcessEventOutcome.PROCESSED
    assert second.customer_id is None
    assert len(state.profile_saves) == 1
    assert state.profiles['customer-1'].events_count == 1


def test_identity_conflict_returns_dlq_and_does_not_update_profile() -> None:
    state = FakeEventProcessingState()
    state.links[(KnownIdentifierType.EMAIL, 'alice@example.com')] = IdentityLink(
        KnownIdentifierType.EMAIL,
        'alice@example.com',
        'customer-1',
    )
    state.links[(KnownIdentifierType.PHONE, '+79990000000')] = IdentityLink(
        KnownIdentifierType.PHONE,
        '+79990000000',
        'customer-2',
    )

    result = run_event(
        state,
        known_event(
            'event-4',
            identifiers=(
                KnownIdentifier(KnownIdentifierType.EMAIL, 'alice@example.com'),
                KnownIdentifier(KnownIdentifierType.PHONE, '+79990000000'),
                KnownIdentifier(KnownIdentifierType.EXTERNAL_USER_ID, 'external-1'),
            ),
        ),
    )

    assert result.outcome == ProcessEventOutcome.SEND_TO_DLQ
    assert result.reason == REASON_IDENTITY_CONFLICT
    assert state.raw_statuses['event-4'] == RawEventProcessingStatus.SENT_TO_DLQ
    assert state.profile_saves == []
    assert state.membership_replacements == []


def test_known_purchase_in_rub_updates_orders_count_and_total_revenue() -> None:
    state = FakeEventProcessingState()

    result = run_event(
        state,
        known_event(
            'event-5',
            EventType.PURCHASE,
            payload={'order_id': 'order-1', 'amount': '1250.50', 'currency': 'RUB'},
        ),
    )

    saved = state.profiles['customer-1']
    assert result.outcome == ProcessEventOutcome.PROCESSED
    assert saved.orders_count == 1
    assert saved.total_revenue == Decimal('1250.50')
    assert state.purchase_records[0].order_id == 'order-1'


def test_duplicate_source_order_id_does_not_update_revenue_twice() -> None:
    state = FakeEventProcessingState()
    original_profile = existing_profile(
        orders_count=1,
        total_revenue=Decimal('100'),
    )
    state.profiles['customer-1'] = original_profile
    link_email_to_customer(state)
    state.purchases[('web', 'order-1')] = ProcessedPurchase(
        source='web',
        order_id='order-1',
        customer_id='customer-1',
        event_id='existing-event',
        amount=Decimal('100'),
        currency=Currency.RUB,
        occurred_at=NOW,
    )

    result = run_event(
        state,
        known_event(
            'event-6',
            EventType.PURCHASE,
            payload={'order_id': 'order-1', 'amount': '100', 'currency': 'RUB'},
        ),
    )

    assert result.outcome == ProcessEventOutcome.PROCESSED
    assert state.raw_statuses['event-6'] == RawEventProcessingStatus.PROCESSED
    assert state.profile_saves == []
    assert state.profiles['customer-1'] == original_profile
    assert state.profiles['customer-1'].orders_count == 1
    assert state.profiles['customer-1'].total_revenue == Decimal('100')


def test_purchase_conflict_returns_dlq_and_does_not_update_profile() -> None:
    state = FakeEventProcessingState()
    original_profile = existing_profile(
        orders_count=1,
        total_revenue=Decimal('100'),
    )
    state.profiles['customer-1'] = original_profile
    link_email_to_customer(state)
    state.purchases[('web', 'order-1')] = ProcessedPurchase(
        source='web',
        order_id='order-1',
        customer_id='customer-1',
        event_id='existing-event',
        amount=Decimal('100'),
        currency=Currency.RUB,
        occurred_at=NOW,
    )

    result = run_event(
        state,
        known_event(
            'event-7',
            EventType.PURCHASE,
            payload={'order_id': 'order-1', 'amount': '101', 'currency': 'RUB'},
        ),
    )

    assert result.outcome == ProcessEventOutcome.SEND_TO_DLQ
    assert result.reason == REASON_PURCHASE_CONFLICT
    assert state.raw_statuses['event-7'] == RawEventProcessingStatus.SENT_TO_DLQ
    assert state.profile_saves == []
    assert state.profiles['customer-1'] == original_profile
    assert state.profiles['customer-1'].orders_count == 1
    assert state.profiles['customer-1'].total_revenue == Decimal('100')


def test_unsupported_currency_returns_dlq() -> None:
    state = FakeEventProcessingState()

    result = run_event(
        state,
        known_event(
            'event-8',
            EventType.PURCHASE,
            payload={'order_id': 'order-1', 'amount': '100', 'currency': 'USD'},
        ),
    )

    assert result.outcome == ProcessEventOutcome.SEND_TO_DLQ
    assert result.reason == REASON_UNSUPPORTED_CURRENCY
    assert state.raw_statuses['event-8'] == RawEventProcessingStatus.SENT_TO_DLQ
    assert state.profile_saves == []
    assert state.purchase_records == []


def test_segment_membership_updates_after_profile_update() -> None:
    state = FakeEventProcessingState()
    state.profiles['customer-1'] = CustomerProfile(
        customer_id='customer-1',
        first_seen_at=NOW,
        last_seen_at=NOW,
        created_at=NOW,
        updated_at=NOW,
        identifiers=CustomerIdentifiers(emails=('alice@example.com',)),
        events_count=2,
        page_views_count=2,
    )
    link_email_to_customer(state)

    result = run_event(state, known_event('event-9'))

    memberships = state.memberships['customer-1']
    segment_ids = frozenset(membership.segment_id for membership in memberships)
    assert result.outcome == ProcessEventOutcome.PROCESSED
    assert state.profiles['customer-1'].events_count == 3
    assert segment_ids == frozenset({SegmentId.ACTIVE, SegmentId.NEW_USER})
    assert state.membership_replacements == [('customer-1', memberships)]


def test_purchase_recalculates_memberships_from_post_purchase_profile() -> None:
    state = FakeEventProcessingState()
    state.profiles['customer-1'] = existing_profile(
        orders_count=2,
        total_revenue=Decimal('90000'),
    )
    link_email_to_customer(state)

    result = run_event(
        state,
        known_event(
            'event-10',
            EventType.PURCHASE,
            payload={'order_id': 'order-3', 'amount': '10000', 'currency': 'RUB'},
        ),
    )

    saved = state.profiles['customer-1']
    memberships = state.memberships['customer-1']
    segment_ids = frozenset(membership.segment_id for membership in memberships)
    assert result.outcome == ProcessEventOutcome.PROCESSED
    assert saved.orders_count == 3
    assert saved.total_revenue == Decimal('100000')
    assert SegmentId.VIP in segment_ids
    assert state.membership_replacements == [('customer-1', memberships)]


def test_unsupported_event_type_returns_dlq() -> None:
    state = FakeEventProcessingState()

    result = run_event(state, known_event('event-11', event_type='identify'))

    assert result.outcome == ProcessEventOutcome.SEND_TO_DLQ
    assert result.reason == REASON_UNSUPPORTED_EVENT_TYPE
    assert state.raw_statuses['event-11'] == RawEventProcessingStatus.SENT_TO_DLQ
    assert state.profile_saves == []


def test_missing_linked_profile_marks_event_failed() -> None:
    state = FakeEventProcessingState()
    link_email_to_customer(state, customer_id='missing-customer')

    result = run_event(state, known_event('event-12'))

    assert result.outcome == ProcessEventOutcome.FAILED
    assert result.reason == REASON_LINKED_PROFILE_NOT_FOUND
    assert state.raw_statuses['event-12'] == RawEventProcessingStatus.FAILED
    assert state.profile_saves == []


def test_invalid_generated_customer_id_marks_event_failed() -> None:
    state = FakeEventProcessingState()
    invalid_service = EventProcessingService(
        FakeEventProcessingUnitOfWork(state),
        customer_id_generator=lambda: '',
        now_provider=lambda: NOW,
    )

    result = asyncio.run(invalid_service.process_event(known_event('event-13')))

    assert result.outcome == ProcessEventOutcome.FAILED
    assert result.reason == REASON_INVALID_GENERATED_CUSTOMER_ID
    assert state.raw_statuses['event-13'] == RawEventProcessingStatus.FAILED
    assert state.profile_saves == []


def test_invalid_purchase_payload_returns_dlq() -> None:
    state = FakeEventProcessingState()

    result = run_event(
        state,
        known_event(
            'event-14',
            EventType.PURCHASE,
            payload={'amount': '100', 'currency': 'RUB'},
        ),
    )

    assert result.outcome == ProcessEventOutcome.SEND_TO_DLQ
    assert result.reason == REASON_INVALID_PURCHASE_PAYLOAD
    assert state.raw_statuses['event-14'] == RawEventProcessingStatus.SENT_TO_DLQ
    assert state.profile_saves == []
    assert state.purchase_records == []


def test_existing_profile_adds_new_phone_identifier_and_link() -> None:
    state = FakeEventProcessingState()
    state.profiles['customer-1'] = existing_profile()
    link_email_to_customer(state)

    result = run_event(
        state,
        known_event(
            'event-15',
            identifiers=(
                KnownIdentifier(KnownIdentifierType.EMAIL, 'alice@example.com'),
                KnownIdentifier(KnownIdentifierType.PHONE, '+79990000000'),
                KnownIdentifier(KnownIdentifierType.EXTERNAL_USER_ID, 'external-1'),
            ),
        ),
    )

    saved = state.profiles['customer-1']
    assert result.outcome == ProcessEventOutcome.PROCESSED
    assert saved.identifiers.emails == ('alice@example.com',)
    assert saved.identifiers.phones == ('+79990000000',)
    assert saved.identifiers.external_user_ids == ('external-1',)
    assert state.saved_links == [
        IdentityLink(KnownIdentifierType.PHONE, '+79990000000', 'customer-1'),
        IdentityLink(KnownIdentifierType.EXTERNAL_USER_ID, 'external-1', 'customer-1'),
    ]


def test_new_profile_keeps_phone_and_external_user_identifiers() -> None:
    state = FakeEventProcessingState()

    result = run_event(
        state,
        known_event(
            'event-16',
            identifiers=(
                KnownIdentifier(KnownIdentifierType.PHONE, '+79990000000'),
                KnownIdentifier(KnownIdentifierType.EXTERNAL_USER_ID, 'external-1'),
            ),
        ),
    )

    saved = state.profiles['customer-1']
    assert result.outcome == ProcessEventOutcome.PROCESSED
    assert saved.identifiers.emails == ()
    assert saved.identifiers.phones == ('+79990000000',)
    assert saved.identifiers.external_user_ids == ('external-1',)


def test_unit_of_work_begin_error_returns_failed_result() -> None:
    state = FakeEventProcessingState(begin_errors_remaining=1)

    result = run_event(state, known_event('event-17'))

    assert result.outcome == ProcessEventOutcome.FAILED
    assert result.reason == REASON_PROCESSING_ERROR
    assert state.raw_statuses == {}
    assert state.rollbacks == 0


def test_record_dependency_error_rolls_back_and_marks_failed() -> None:
    state = FakeEventProcessingState(record_errors_remaining=1)

    result = run_event(state, known_event('event-18'))

    assert result.outcome == ProcessEventOutcome.FAILED
    assert result.reason == REASON_PROCESSING_ERROR
    assert state.raw_statuses['event-18'] == RawEventProcessingStatus.FAILED
    assert state.raw_reasons['event-18'] == REASON_PROCESSING_ERROR
    assert state.rollbacks == 1


def test_commit_dependency_error_rolls_back_and_marks_failed() -> None:
    state = FakeEventProcessingState(commit_errors_remaining=1)

    result = run_event(state, known_event('event-19'))

    assert result.outcome == ProcessEventOutcome.FAILED
    assert result.reason == REASON_PROCESSING_ERROR
    assert state.raw_statuses['event-19'] == RawEventProcessingStatus.FAILED
    assert state.raw_reasons['event-19'] == REASON_PROCESSING_ERROR
    assert state.rollbacks == 1


def test_record_dependency_error_ignores_rollback_failure() -> None:
    state = FakeEventProcessingState(
        record_errors_remaining=1,
        rollback_errors_remaining=1,
    )

    result = run_event(state, known_event('event-20'))

    assert result.outcome == ProcessEventOutcome.FAILED
    assert result.reason == REASON_PROCESSING_ERROR
    assert state.raw_statuses['event-20'] == RawEventProcessingStatus.FAILED
    assert state.rollbacks == 0


def test_commit_error_while_marking_failed_rolls_back_again() -> None:
    state = FakeEventProcessingState(commit_errors_remaining=2)

    result = run_event(state, known_event('event-21'))

    assert result.outcome == ProcessEventOutcome.FAILED
    assert result.reason == REASON_PROCESSING_ERROR
    assert state.raw_statuses['event-21'] == RawEventProcessingStatus.FAILED
    assert state.rollbacks == 2


def test_dependency_error_when_marking_failed_still_returns_failed() -> None:
    state = FakeEventProcessingState(
        begin_error_on_call=2,
        record_errors_remaining=1,
    )

    result = run_event(state, known_event('event-22'))

    assert result.outcome == ProcessEventOutcome.FAILED
    assert result.reason == REASON_PROCESSING_ERROR
    assert state.raw_statuses == {}


def test_profile_update_domain_error_returns_dlq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = FakeEventProcessingState()

    def raise_domain_error(
        profile: CustomerProfile, event: RawEvent
    ) -> CustomerProfile:
        raise DomainError('invalid profile update')

    monkeypatch.setattr(
        process_event_module,
        'update_profile_from_event',
        raise_domain_error,
    )

    result = run_event(state, known_event('event-23'))

    assert result.outcome == ProcessEventOutcome.SEND_TO_DLQ
    assert result.reason == REASON_INVALID_PURCHASE_PAYLOAD
    assert state.raw_statuses['event-23'] == RawEventProcessingStatus.SENT_TO_DLQ


def test_profile_update_unsupported_currency_returns_dlq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = FakeEventProcessingState()

    def raise_currency_error(
        profile: CustomerProfile, event: RawEvent
    ) -> CustomerProfile:
        raise UnsupportedCurrencyError('purchase currency must be RUB')

    monkeypatch.setattr(
        process_event_module,
        'update_profile_from_event',
        raise_currency_error,
    )

    result = run_event(state, known_event('event-24'))

    assert result.outcome == ProcessEventOutcome.SEND_TO_DLQ
    assert result.reason == REASON_UNSUPPORTED_CURRENCY
    assert state.raw_statuses['event-24'] == RawEventProcessingStatus.SENT_TO_DLQ
