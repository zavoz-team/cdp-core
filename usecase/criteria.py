from dataclasses import dataclass

from domain.segment import SegmentId
from usecase.error import UseCaseValidationError


@dataclass(frozen=True, slots=True)
class ProfileListCriteria:
    limit: int = 50
    offset: int = 0
    segment_id: SegmentId | None = None
    email: str | None = None
    phone: str | None = None
    external_user_id: str | None = None

    def __post_init__(self) -> None:
        validate_pagination(self.limit, self.offset)


def validate_pagination(limit: int, offset: int) -> None:
    if limit < 0:
        raise UseCaseValidationError('limit must not be negative')

    if offset < 0:
        raise UseCaseValidationError('offset must not be negative')
