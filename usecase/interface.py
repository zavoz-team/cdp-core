from typing import Protocol

from domain.event import RawEvent
from domain.export_job import (
    ActivationDelivery,
    ActivationDestination,
    ActivationJob,
    ActivationJobStatus,
)
from domain.identity import IdentityLink, KnownIdentifier
from domain.profile import CustomerProfile
from domain.segment import SegmentDefinition, SegmentId, SegmentMembership
from usecase.criteria import ProfileListCriteria
from usecase.dto import (
    OutboundWebhookDeliveryResult,
    ProcessedPurchase,
    PurchaseRecordResult,
    RawEventRecordResult,
    SegmentExportPayload,
)


class RawEventRepository(Protocol):
    async def get_by_event_id(self, event_id: str) -> RawEvent | None: ...

    async def record_received(self, raw_event: RawEvent) -> RawEventRecordResult: ...

    async def mark_processed(self, event_id: str) -> None: ...

    async def mark_ignored_anonymous(self, event_id: str) -> None: ...

    async def mark_sent_to_dlq(self, event_id: str, reason: str) -> None: ...

    async def mark_failed(self, event_id: str, reason: str) -> None: ...


class CustomerProfileRepository(Protocol):
    async def get_by_customer_id(self, customer_id: str) -> CustomerProfile | None: ...

    async def get_many_by_customer_ids(
        self,
        customer_ids: tuple[str, ...],
    ) -> tuple[CustomerProfile, ...]: ...

    async def list_profiles(
        self,
        criteria: ProfileListCriteria,
    ) -> tuple[CustomerProfile, ...]: ...

    async def count_profiles(self, criteria: ProfileListCriteria) -> int: ...

    async def save_profile(self, profile: CustomerProfile) -> None: ...


class IdentityRepository(Protocol):
    async def get_link(self, identifier: KnownIdentifier) -> IdentityLink | None: ...

    async def find_links(
        self,
        identifiers: tuple[KnownIdentifier, ...],
    ) -> tuple[IdentityLink, ...]: ...

    async def save_links(self, links: tuple[IdentityLink, ...]) -> None: ...


class PurchaseRepository(Protocol):
    async def get_by_source_order_id(
        self,
        source: str,
        order_id: str,
    ) -> ProcessedPurchase | None: ...

    async def record_processed_purchase(
        self,
        purchase: ProcessedPurchase,
    ) -> PurchaseRecordResult: ...


class SegmentRepository(Protocol):
    async def get_definition(
        self, segment_id: SegmentId
    ) -> SegmentDefinition | None: ...

    async def list_definitions(
        self,
        include_disabled: bool = False,
    ) -> tuple[SegmentDefinition, ...]: ...

    async def count_members(self, segment_id: SegmentId) -> int: ...

    async def list_memberships(
        self,
        segment_id: SegmentId,
        limit: int,
        offset: int,
    ) -> tuple[SegmentMembership, ...]: ...

    async def list_member_profiles(
        self,
        segment_id: SegmentId,
        limit: int,
        offset: int,
    ) -> tuple[CustomerProfile, ...]: ...

    async def replace_profile_memberships(
        self,
        customer_id: str,
        memberships: tuple[SegmentMembership, ...],
    ) -> None: ...


class EventProcessingTransaction(Protocol):
    raw_events: RawEventRepository
    customer_profiles: CustomerProfileRepository
    identities: IdentityRepository
    purchases: PurchaseRepository
    segments: SegmentRepository

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


class EventProcessingUnitOfWork(Protocol):
    async def begin(self) -> EventProcessingTransaction: ...


class ActivationJobRepository(Protocol):
    async def get_by_job_id(self, job_id: str) -> ActivationJob | None: ...

    async def list_jobs(
        self,
        limit: int = 50,
        offset: int = 0,
        segment_id: SegmentId | None = None,
        status: ActivationJobStatus | None = None,
    ) -> tuple[ActivationJob, ...]: ...

    async def count_jobs(
        self,
        limit: int = 50,
        offset: int = 0,
        segment_id: SegmentId | None = None,
        status: ActivationJobStatus | None = None,
    ) -> int: ...

    async def save_job(self, job: ActivationJob) -> None: ...


class ActivationDeliveryRepository(Protocol):
    async def get_for_job(self, job_id: str) -> ActivationDelivery | None: ...

    async def save_delivery(self, delivery: ActivationDelivery) -> None: ...


class ExportTransaction(Protocol):
    jobs: ActivationJobRepository
    deliveries: ActivationDeliveryRepository

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


class ExportUnitOfWork(Protocol):
    async def begin(self) -> ExportTransaction: ...


class OutboundWebhookGateway(Protocol):
    async def send_segment_export(
        self,
        destination: ActivationDestination,
        payload: SegmentExportPayload,
    ) -> OutboundWebhookDeliveryResult: ...
