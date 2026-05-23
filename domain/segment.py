from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from domain.error import DomainError


class SegmentId(StrEnum):
    NEW_USER = 'new_user'
    ACTIVE = 'active'
    VIP = 'vip'


@dataclass(frozen=True, slots=True)
class SegmentDefinition:
    segment_id: SegmentId
    name: str
    description: str
    is_active: bool = True

    def __post_init__(self) -> None:
        try:
            segment_id = SegmentId(self.segment_id)
        except ValueError as error:
            raise DomainError(f'unsupported segment_id: {self.segment_id}') from error

        _validate_non_empty(self.name, 'name')
        _validate_non_empty(self.description, 'description')
        object.__setattr__(self, 'segment_id', segment_id)


@dataclass(frozen=True, slots=True)
class SegmentMembership:
    segment_id: SegmentId
    customer_id: str
    member_since: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        try:
            segment_id = SegmentId(self.segment_id)
        except ValueError as error:
            raise DomainError(f'unsupported segment_id: {self.segment_id}') from error

        _validate_non_empty(self.customer_id, 'customer_id')
        object.__setattr__(self, 'segment_id', segment_id)


def _validate_non_empty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise DomainError(f'{field_name} must be non-empty')


STATIC_SEGMENT_DEFINITIONS: tuple[SegmentDefinition, ...] = (
    SegmentDefinition(
        segment_id=SegmentId.NEW_USER,
        name='New User',
        description='Customers with no orders first seen within 7 days',
    ),
    SegmentDefinition(
        segment_id=SegmentId.ACTIVE,
        name='Active',
        description='Customers seen within 30 days with at least 3 events',
    ),
    SegmentDefinition(
        segment_id=SegmentId.VIP,
        name='VIP',
        description='Customers with revenue >= 100000 RUB or 3+ orders',
    ),
)
