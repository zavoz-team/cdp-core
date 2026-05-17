from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Mapping

from domain.event import RawEventProcessingStatus
from domain.export_job import ActivationDelivery, ActivationJob
from domain.profile import Currency, CustomerProfile
from domain.segment import SegmentDefinition, SegmentId, SegmentMembership
from usecase.error import UseCaseValidationError


class ProcessEventOutcome(StrEnum):
    PROCESSED = 'processed'
    IGNORED_ANONYMOUS = 'ignored_anonymous'
    SEND_TO_DLQ = 'send_to_dlq'
    FAILED = 'failed'


class RawEventRecordOutcome(StrEnum):
    CREATED = 'created'
    DUPLICATE = 'duplicate'


class PurchaseRecordOutcome(StrEnum):
    RECORDED = 'recorded'
    # Same source + order_id already exists for the same logical purchase data.
    DUPLICATE = 'duplicate'
    # Same source + order_id exists but conflicts on purchase facts.
    CONFLICT = 'conflict'


class HealthStatus(StrEnum):
    OK = 'ok'
    DEGRADED = 'degraded'


@dataclass(frozen=True, slots=True)
class ProcessEventResult:
    event_id: str
    outcome: ProcessEventOutcome
    raw_event_status: RawEventProcessingStatus
    customer_id: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        outcome = ProcessEventOutcome(self.outcome)
        raw_event_status = RawEventProcessingStatus(self.raw_event_status)
        object.__setattr__(self, 'outcome', outcome)
        object.__setattr__(self, 'raw_event_status', raw_event_status)

        expected_status = _PROCESS_EVENT_STATUS_BY_OUTCOME[outcome]
        if raw_event_status != expected_status:
            raise UseCaseValidationError(
                f'process event outcome {outcome} requires raw status {expected_status}'
            )

        if outcome in _PROCESS_EVENT_REASON_REQUIRED and not _has_text(self.reason):
            raise UseCaseValidationError(
                f'process event outcome {outcome} requires reason'
            )

        if outcome in _PROCESS_EVENT_REASON_FORBIDDEN and self.reason is not None:
            raise UseCaseValidationError(
                f'process event outcome {outcome} must not include reason'
            )

    @classmethod
    def processed(cls, event_id: str, customer_id: str | None) -> 'ProcessEventResult':
        return cls(
            event_id=event_id,
            outcome=ProcessEventOutcome.PROCESSED,
            raw_event_status=RawEventProcessingStatus.PROCESSED,
            customer_id=customer_id,
        )

    @classmethod
    def ignored_anonymous(cls, event_id: str) -> 'ProcessEventResult':
        return cls(
            event_id=event_id,
            outcome=ProcessEventOutcome.IGNORED_ANONYMOUS,
            raw_event_status=RawEventProcessingStatus.IGNORED_ANONYMOUS,
        )

    @classmethod
    def send_to_dlq(cls, event_id: str, reason: str) -> 'ProcessEventResult':
        return cls(
            event_id=event_id,
            outcome=ProcessEventOutcome.SEND_TO_DLQ,
            raw_event_status=RawEventProcessingStatus.SENT_TO_DLQ,
            reason=reason,
        )

    @classmethod
    def failed(cls, event_id: str, reason: str) -> 'ProcessEventResult':
        return cls(
            event_id=event_id,
            outcome=ProcessEventOutcome.FAILED,
            raw_event_status=RawEventProcessingStatus.FAILED,
            reason=reason,
        )


_PROCESS_EVENT_STATUS_BY_OUTCOME = {
    ProcessEventOutcome.PROCESSED: RawEventProcessingStatus.PROCESSED,
    ProcessEventOutcome.IGNORED_ANONYMOUS: RawEventProcessingStatus.IGNORED_ANONYMOUS,
    ProcessEventOutcome.SEND_TO_DLQ: RawEventProcessingStatus.SENT_TO_DLQ,
    ProcessEventOutcome.FAILED: RawEventProcessingStatus.FAILED,
}

_PROCESS_EVENT_REASON_REQUIRED = {
    ProcessEventOutcome.SEND_TO_DLQ,
    ProcessEventOutcome.FAILED,
}

_PROCESS_EVENT_REASON_FORBIDDEN = {
    ProcessEventOutcome.PROCESSED,
    ProcessEventOutcome.IGNORED_ANONYMOUS,
}

_PURCHASE_RECORD_EXISTING_PURCHASE_REQUIRED = {
    PurchaseRecordOutcome.DUPLICATE,
    PurchaseRecordOutcome.CONFLICT,
}


@dataclass(frozen=True, slots=True)
class RawEventRecordResult:
    outcome: RawEventRecordOutcome
    event_id: str


@dataclass(frozen=True, slots=True)
class PurchaseRecordResult:
    outcome: PurchaseRecordOutcome
    source: str
    order_id: str
    existing_purchase: 'ProcessedPurchase | None' = None

    def __post_init__(self) -> None:
        outcome = PurchaseRecordOutcome(self.outcome)
        object.__setattr__(self, 'outcome', outcome)

        if outcome in _PURCHASE_RECORD_EXISTING_PURCHASE_REQUIRED:
            if self.existing_purchase is None:
                raise UseCaseValidationError(
                    f'purchase record outcome {outcome} requires existing_purchase'
                )
        elif self.existing_purchase is not None:
            raise UseCaseValidationError(
                f'purchase record outcome {outcome} must not include existing_purchase'
            )


@dataclass(frozen=True, slots=True)
class GetProfileResult:
    profile: CustomerProfile


@dataclass(frozen=True, slots=True)
class ListProfilesResult:
    profiles: tuple[CustomerProfile, ...]
    total: int
    limit: int
    offset: int


@dataclass(frozen=True, slots=True)
class SegmentSummary:
    definition: SegmentDefinition
    members_count: int


@dataclass(frozen=True, slots=True)
class ListSegmentsResult:
    segments: tuple[SegmentSummary, ...]


@dataclass(frozen=True, slots=True)
class GetSegmentMembersResult:
    segment: SegmentDefinition
    members: tuple[CustomerProfile, ...]
    total: int
    limit: int
    offset: int


@dataclass(frozen=True, slots=True)
class ProcessedPurchase:
    source: str
    order_id: str
    customer_id: str
    event_id: str
    amount: Decimal
    currency: Currency
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class SegmentExportPayload:
    job_id: str
    segment_id: SegmentId
    exported_at: datetime
    members_count: int
    members: tuple[CustomerProfile, ...]


@dataclass(frozen=True, slots=True)
class OutboundWebhookDeliveryResult:
    delivered: bool
    response_status_code: int | None = None
    error_reason: str | None = None

    def __post_init__(self) -> None:
        if self.delivered and self.error_reason is not None:
            raise UseCaseValidationError(
                'delivered webhook result must not include error_reason'
            )

        if self.delivered and not _is_success_status(self.response_status_code):
            raise UseCaseValidationError(
                'delivered webhook result response_status_code must be 2xx'
            )

        if not self.delivered and not _has_text(self.error_reason):
            raise UseCaseValidationError(
                'failed webhook result must include error_reason'
            )

    @classmethod
    def succeeded(
        cls,
        response_status_code: int | None = None,
    ) -> 'OutboundWebhookDeliveryResult':
        return cls(delivered=True, response_status_code=response_status_code)

    @classmethod
    def failed(
        cls,
        error_reason: str,
        response_status_code: int | None = None,
    ) -> 'OutboundWebhookDeliveryResult':
        return cls(
            delivered=False,
            response_status_code=response_status_code,
            error_reason=error_reason,
        )


@dataclass(frozen=True, slots=True)
class ExportSegmentResult:
    job: ActivationJob
    delivery: ActivationDelivery | None = None


@dataclass(frozen=True, slots=True)
class ListExportJobsResult:
    jobs: tuple[ActivationJob, ...]
    total: int
    limit: int
    offset: int


@dataclass(frozen=True, slots=True)
class GetExportJobResult:
    job: ActivationJob
    delivery: ActivationDelivery | None = None


@dataclass(frozen=True, slots=True)
class GetHealthStatusResult:
    status: HealthStatus = HealthStatus.OK
    service: str = 'cdp-core'
    details: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SegmentMembershipChange:
    customer_id: str
    memberships: tuple[SegmentMembership, ...]


def _has_text(value: str | None) -> bool:
    return value is not None and bool(value.strip())


def _is_success_status(response_status_code: int | None) -> bool:
    return response_status_code is None or 200 <= response_status_code <= 299
