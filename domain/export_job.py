from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping

from domain.error import ActivationDeliveryError, DomainError
from domain.segment import SegmentId


class ActivationJobStatus(StrEnum):
    PENDING = 'pending'
    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    PARTIALLY_FAILED = 'partially_failed'


class ActivationDeliveryStatus(StrEnum):
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


class DestinationType(StrEnum):
    WEBHOOK = 'webhook'


@dataclass(frozen=True, slots=True)
class ActivationDestination:
    destination_type: DestinationType
    url: str

    def __post_init__(self) -> None:
        try:
            destination_type = DestinationType(self.destination_type)
        except ValueError as error:
            raise DomainError(
                f'unsupported destination type: {self.destination_type}'
            ) from error

        _validate_non_empty(self.url, 'url')
        object.__setattr__(self, 'destination_type', destination_type)


@dataclass(frozen=True, slots=True)
class ActivationJob:
    job_id: str
    segment_id: SegmentId
    destination: ActivationDestination
    requested_at: datetime
    status: ActivationJobStatus = ActivationJobStatus.PENDING
    members_count: int = 0
    requested_by: str | None = None
    actor_context: Mapping[str, object] = field(default_factory=dict)
    completed_at: datetime | None = None
    error_reason: str | None = None

    def __post_init__(self) -> None:
        _validate_non_empty(self.job_id, 'job_id')
        _validate_optional_non_empty(self.requested_by, 'requested_by')
        _validate_count(self.members_count, 'members_count')

        object.__setattr__(self, 'segment_id', _segment_id_value(self.segment_id))
        object.__setattr__(self, 'status', _job_status_value(self.status))
        object.__setattr__(
            self, 'actor_context', MappingProxyType(dict(self.actor_context))
        )

    @property
    def is_terminal(self) -> bool:
        return self.status in {
            ActivationJobStatus.SUCCEEDED,
            ActivationJobStatus.FAILED,
            ActivationJobStatus.PARTIALLY_FAILED,
        }


@dataclass(frozen=True, slots=True)
class ActivationDelivery:
    delivery_id: str
    job_id: str
    segment_id: SegmentId
    destination: ActivationDestination
    attempt_number: int
    members_count: int
    status: ActivationDeliveryStatus
    requested_at: datetime
    completed_at: datetime | None = None
    response_status_code: int | None = None
    error_reason: str | None = None

    def __post_init__(self) -> None:
        _validate_non_empty(self.delivery_id, 'delivery_id')
        _validate_non_empty(self.job_id, 'job_id')
        _validate_count(self.members_count, 'members_count')

        if self.attempt_number != 1:
            raise ActivationDeliveryError(
                'attempt_number must be exactly one for MVP delivery'
            )

        status = _delivery_status_value(self.status)
        if status == ActivationDeliveryStatus.FAILED and not self.error_reason:
            raise ActivationDeliveryError('failed delivery must include error_reason')

        object.__setattr__(self, 'segment_id', _segment_id_value(self.segment_id))
        object.__setattr__(self, 'status', status)


def _segment_id_value(segment_id: SegmentId) -> SegmentId:
    try:
        return SegmentId(segment_id)
    except ValueError as error:
        raise DomainError(f'unsupported segment_id: {segment_id}') from error


def _job_status_value(status: ActivationJobStatus) -> ActivationJobStatus:
    try:
        return ActivationJobStatus(status)
    except ValueError as error:
        raise DomainError(f'unsupported activation job status: {status}') from error


def _delivery_status_value(
    status: ActivationDeliveryStatus,
) -> ActivationDeliveryStatus:
    try:
        return ActivationDeliveryStatus(status)
    except ValueError as error:
        raise ActivationDeliveryError(
            f'unsupported activation delivery status: {status}'
        ) from error


def _validate_count(value: int, field_name: str) -> None:
    if value < 0:
        raise DomainError(f'{field_name} must not be negative')


def _validate_non_empty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise DomainError(f'{field_name} must be non-empty')


def _validate_optional_non_empty(value: str | None, field_name: str) -> None:
    if value is not None and not value.strip():
        raise DomainError(f'{field_name} must be non-empty when provided')
