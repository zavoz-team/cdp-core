from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime

from domain.error import DomainError, UnsupportedCurrencyError
from domain.event import EventType, RawEvent
from domain.identity import (
    CustomerIdentifiers,
    IdentityLink,
    KnownIdentifier,
    KnownIdentifierType,
)
from domain.profile import CustomerProfile
from domain.rules import (
    update_profile_from_event,
    update_profile_segments,
    validate_purchase_payload,
)
from domain.segment import SegmentMembership
from usecase.dto import (
    ProcessedPurchase,
    ProcessEventResult,
    PurchaseRecordOutcome,
    RawEventRecordOutcome,
)
from usecase.error import UseCaseDependencyError
from usecase.interface import EventProcessingTransaction, EventProcessingUnitOfWork

REASON_IDENTITY_CONFLICT = 'identity_conflict'
REASON_INVALID_EVENT_IDENTIFIERS = 'invalid_event_identifiers'
REASON_INVALID_GENERATED_CUSTOMER_ID = 'invalid_generated_customer_id'
REASON_INVALID_PURCHASE_PAYLOAD = 'invalid_purchase_payload'
REASON_LINKED_PROFILE_NOT_FOUND = 'linked_profile_not_found'
REASON_PURCHASE_CONFLICT = 'purchase_conflict'
REASON_PROCESSING_ERROR = 'processing_error'
REASON_UNSUPPORTED_CURRENCY = 'unsupported_currency'
REASON_UNSUPPORTED_EVENT_TYPE = 'unsupported_event_type'


@dataclass(frozen=True, slots=True)
class _KnownProfile:
    profile: CustomerProfile
    links: tuple[IdentityLink, ...]


class EventProcessingService:
    def __init__(
        self,
        unit_of_work: EventProcessingUnitOfWork,
        customer_id_generator: Callable[[], str],
        now_provider: Callable[[], datetime],
    ) -> None:
        self._unit_of_work = unit_of_work
        self._customer_id_generator = customer_id_generator
        self._now_provider = now_provider

    async def process_event(self, raw_event: RawEvent) -> ProcessEventResult:
        try:
            transaction = await self._unit_of_work.begin()
        except UseCaseDependencyError:
            return ProcessEventResult.failed(
                raw_event.event_id, REASON_PROCESSING_ERROR
            )

        try:
            result = await self._process_attempt(raw_event, transaction)
        except UseCaseDependencyError:
            await self._rollback(transaction)
            return await self._mark_failed_after_dependency_error(raw_event.event_id)

        try:
            await transaction.commit()
        except UseCaseDependencyError:
            await self._rollback(transaction)
            return await self._mark_failed_after_dependency_error(raw_event.event_id)

        return result

    async def _process_attempt(
        self,
        raw_event: RawEvent,
        transaction: EventProcessingTransaction,
    ) -> ProcessEventResult:
        record_result = await transaction.raw_events.record_received(raw_event)

        if record_result.outcome == RawEventRecordOutcome.DUPLICATE:
            return ProcessEventResult.processed(raw_event.event_id, customer_id=None)

        return await self._process_recorded_event(raw_event, transaction)

    async def _process_recorded_event(
        self,
        raw_event: RawEvent,
        transaction: EventProcessingTransaction,
    ) -> ProcessEventResult:
        event_type = raw_event.supported_event_type()
        if event_type is None:
            return await self._send_to_dlq(
                transaction,
                raw_event.event_id,
                REASON_UNSUPPORTED_EVENT_TYPE,
            )

        if raw_event.identifiers.is_anonymous_only:
            await transaction.raw_events.mark_ignored_anonymous(raw_event.event_id)
            return ProcessEventResult.ignored_anonymous(raw_event.event_id)

        return await self._process_known_event(raw_event, event_type, transaction)

    async def _process_known_event(
        self,
        raw_event: RawEvent,
        event_type: EventType,
        transaction: EventProcessingTransaction,
    ) -> ProcessEventResult:
        known_identifiers = _unique_known_identifiers(raw_event.identifiers.known)
        if not known_identifiers:
            return await self._send_to_dlq(
                transaction,
                raw_event.event_id,
                REASON_INVALID_EVENT_IDENTIFIERS,
            )

        resolved = await self._resolve_known_profile(
            raw_event,
            known_identifiers,
            transaction,
        )
        if isinstance(resolved, ProcessEventResult):
            return resolved

        if event_type == EventType.PURCHASE:
            purchase_result = await self._record_purchase(
                raw_event,
                resolved.profile.customer_id,
                transaction,
            )
            if purchase_result is not None:
                return purchase_result

        return await self._update_known_profile(
            raw_event,
            known_identifiers,
            resolved,
            transaction,
        )

    async def _resolve_known_profile(
        self,
        raw_event: RawEvent,
        known_identifiers: tuple[KnownIdentifier, ...],
        transaction: EventProcessingTransaction,
    ) -> _KnownProfile | ProcessEventResult:
        links = await transaction.identities.find_links(known_identifiers)
        customer_ids = frozenset(link.customer_id for link in links)
        if len(customer_ids) > 1:
            return await self._send_to_dlq(
                transaction,
                raw_event.event_id,
                REASON_IDENTITY_CONFLICT,
            )

        profile, failed_reason = await self._resolve_profile(
            raw_event,
            known_identifiers,
            customer_ids,
            transaction,
        )
        if profile is None:
            return await self._failed(
                transaction,
                raw_event.event_id,
                failed_reason or REASON_LINKED_PROFILE_NOT_FOUND,
            )

        return _KnownProfile(profile=profile, links=links)

    async def _update_known_profile(
        self,
        raw_event: RawEvent,
        known_identifiers: tuple[KnownIdentifier, ...],
        resolved: _KnownProfile,
        transaction: EventProcessingTransaction,
    ) -> ProcessEventResult:
        new_links = _new_identity_links(
            known_identifiers,
            resolved.links,
            resolved.profile.customer_id,
        )
        now = self._now_provider()

        try:
            updated_profile = update_profile_segments(
                update_profile_from_event(resolved.profile, raw_event),
                now,
            )
        except UnsupportedCurrencyError:
            return await self._send_to_dlq(
                transaction,
                raw_event.event_id,
                REASON_UNSUPPORTED_CURRENCY,
            )
        except DomainError:
            return await self._send_to_dlq(
                transaction,
                raw_event.event_id,
                REASON_INVALID_PURCHASE_PAYLOAD,
            )

        memberships = _segment_memberships(updated_profile, now)
        await transaction.customer_profiles.save_profile(updated_profile)
        await transaction.segments.replace_profile_memberships(
            updated_profile.customer_id,
            memberships,
        )
        if new_links:
            await transaction.identities.save_links(new_links)

        await transaction.raw_events.mark_processed(raw_event.event_id)
        return ProcessEventResult.processed(
            raw_event.event_id, updated_profile.customer_id
        )

    async def _resolve_profile(
        self,
        raw_event: RawEvent,
        known_identifiers: tuple[KnownIdentifier, ...],
        customer_ids: frozenset[str],
        transaction: EventProcessingTransaction,
    ) -> tuple[CustomerProfile | None, str | None]:
        if not customer_ids:
            try:
                return CustomerProfile(
                    customer_id=self._customer_id_generator(),
                    first_seen_at=raw_event.occurred_at,
                    last_seen_at=raw_event.occurred_at,
                    created_at=raw_event.created_at,
                    updated_at=raw_event.created_at,
                    identifiers=_customer_identifiers_from_known(known_identifiers),
                ), None
            except DomainError:
                return None, REASON_INVALID_GENERATED_CUSTOMER_ID

        customer_id = next(iter(customer_ids))
        profile = await transaction.customer_profiles.get_by_customer_id(customer_id)
        if profile is None:
            return None, REASON_LINKED_PROFILE_NOT_FOUND

        return _profile_with_known_identifiers(profile, known_identifiers), None

    async def _record_purchase(
        self,
        raw_event: RawEvent,
        customer_id: str,
        transaction: EventProcessingTransaction,
    ) -> ProcessEventResult | None:
        try:
            purchase = validate_purchase_payload(raw_event.payload)
        except UnsupportedCurrencyError:
            return await self._send_to_dlq(
                transaction,
                raw_event.event_id,
                REASON_UNSUPPORTED_CURRENCY,
            )
        except DomainError:
            return await self._send_to_dlq(
                transaction,
                raw_event.event_id,
                REASON_INVALID_PURCHASE_PAYLOAD,
            )

        record_result = await transaction.purchases.record_processed_purchase(
            ProcessedPurchase(
                source=raw_event.source,
                order_id=purchase.order_id,
                customer_id=customer_id,
                event_id=raw_event.event_id,
                amount=purchase.amount,
                currency=purchase.currency,
                occurred_at=raw_event.occurred_at,
            )
        )

        if record_result.outcome == PurchaseRecordOutcome.DUPLICATE:
            await transaction.raw_events.mark_processed(raw_event.event_id)
            return ProcessEventResult.processed(raw_event.event_id, customer_id)

        if record_result.outcome == PurchaseRecordOutcome.CONFLICT:
            return await self._send_to_dlq(
                transaction,
                raw_event.event_id,
                REASON_PURCHASE_CONFLICT,
            )

        return None

    async def _send_to_dlq(
        self,
        transaction: EventProcessingTransaction,
        event_id: str,
        reason: str,
    ) -> ProcessEventResult:
        await transaction.raw_events.mark_sent_to_dlq(event_id, reason)
        return ProcessEventResult.send_to_dlq(event_id, reason)

    async def _failed(
        self,
        transaction: EventProcessingTransaction,
        event_id: str,
        reason: str,
    ) -> ProcessEventResult:
        await transaction.raw_events.mark_failed(event_id, reason)
        return ProcessEventResult.failed(event_id, reason)

    async def _mark_failed_after_dependency_error(
        self,
        event_id: str,
    ) -> ProcessEventResult:
        try:
            transaction = await self._unit_of_work.begin()
            try:
                await transaction.raw_events.mark_failed(
                    event_id,
                    REASON_PROCESSING_ERROR,
                )
                await transaction.commit()
            except UseCaseDependencyError:
                await self._rollback(transaction)
        except UseCaseDependencyError:
            pass

        return ProcessEventResult.failed(event_id, REASON_PROCESSING_ERROR)

    async def _rollback(self, transaction: EventProcessingTransaction) -> None:
        try:
            await transaction.rollback()
        except UseCaseDependencyError:
            pass


def _unique_known_identifiers(
    identifiers: tuple[KnownIdentifier, ...],
) -> tuple[KnownIdentifier, ...]:
    result: list[KnownIdentifier] = []
    seen: set[tuple[KnownIdentifierType, str]] = set()
    for identifier in identifiers:
        key = _known_identifier_key(identifier)
        if key not in seen:
            seen.add(key)
            result.append(identifier)

    return tuple(result)


def _customer_identifiers_from_known(
    identifiers: tuple[KnownIdentifier, ...],
) -> CustomerIdentifiers:
    emails: list[str] = []
    phones: list[str] = []
    external_user_ids: list[str] = []

    for identifier in identifiers:
        if identifier.identifier_type == KnownIdentifierType.EMAIL:
            emails.append(identifier.value)
        elif identifier.identifier_type == KnownIdentifierType.PHONE:
            phones.append(identifier.value)
        elif identifier.identifier_type == KnownIdentifierType.EXTERNAL_USER_ID:
            external_user_ids.append(identifier.value)

    return CustomerIdentifiers(
        emails=tuple(emails),
        phones=tuple(phones),
        external_user_ids=tuple(external_user_ids),
    )


def _profile_with_known_identifiers(
    profile: CustomerProfile,
    identifiers: tuple[KnownIdentifier, ...],
) -> CustomerProfile:
    merged_identifiers = _merge_customer_identifiers(profile.identifiers, identifiers)
    if merged_identifiers == profile.identifiers:
        return profile

    return replace(profile, identifiers=merged_identifiers)


def _merge_customer_identifiers(
    current: CustomerIdentifiers,
    identifiers: tuple[KnownIdentifier, ...],
) -> CustomerIdentifiers:
    emails = list(current.emails)
    phones = list(current.phones)
    external_user_ids = list(current.external_user_ids)

    for identifier in identifiers:
        if identifier.identifier_type == KnownIdentifierType.EMAIL:
            _append_missing(emails, identifier.value)
        elif identifier.identifier_type == KnownIdentifierType.PHONE:
            _append_missing(phones, identifier.value)
        elif identifier.identifier_type == KnownIdentifierType.EXTERNAL_USER_ID:
            _append_missing(external_user_ids, identifier.value)

    return CustomerIdentifiers(
        emails=tuple(emails),
        phones=tuple(phones),
        external_user_ids=tuple(external_user_ids),
    )


def _append_missing(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _new_identity_links(
    identifiers: tuple[KnownIdentifier, ...],
    existing_links: tuple[IdentityLink, ...],
    customer_id: str,
) -> tuple[IdentityLink, ...]:
    linked_keys = {
        (link.identity_type, link.identity_value)
        for link in existing_links
        if link.customer_id == customer_id
    }
    return tuple(
        IdentityLink(
            identity_type=identifier.identifier_type,
            identity_value=identifier.value,
            customer_id=customer_id,
        )
        for identifier in identifiers
        if _known_identifier_key(identifier) not in linked_keys
    )


def _segment_memberships(
    profile: CustomerProfile,
    now: datetime,
) -> tuple[SegmentMembership, ...]:
    return tuple(
        SegmentMembership(
            segment_id=segment_id,
            customer_id=profile.customer_id,
            member_since=now,
            updated_at=now,
        )
        for segment_id in sorted(profile.current_segments, key=str)
    )


def _known_identifier_key(
    identifier: KnownIdentifier,
) -> tuple[KnownIdentifierType, str]:
    return (identifier.identifier_type, identifier.value)
