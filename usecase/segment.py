from domain.segment import SegmentId
from usecase.criteria import validate_pagination
from usecase.dto import (
    GetSegmentMembersResult,
    ListSegmentsResult,
    SegmentSummary,
)
from usecase.error import SegmentNotFoundUseCaseError
from usecase.interface import SegmentRepository


class SegmentService:
    def __init__(self, segment_repository: SegmentRepository) -> None:
        self._segment_repository = segment_repository

    async def list_segments(
        self,
        include_disabled: bool = False,
    ) -> ListSegmentsResult:
        definitions = await self._segment_repository.list_definitions(
            include_disabled=include_disabled
        )
        summaries: list[SegmentSummary] = []
        for definition in definitions:
            members_count = await self._segment_repository.count_members(
                definition.segment_id
            )
            summaries.append(
                SegmentSummary(definition=definition, members_count=members_count)
            )

        return ListSegmentsResult(segments=tuple(summaries))

    async def get_segment_members(
        self,
        segment_id: SegmentId,
        limit: int = 50,
        offset: int = 0,
    ) -> GetSegmentMembersResult:
        validate_pagination(limit, offset)
        definition = await self._segment_repository.get_definition(segment_id)
        if definition is None:
            raise SegmentNotFoundUseCaseError(f'segment not found: {segment_id}')

        members = await self._segment_repository.list_member_profiles(
            segment_id,
            limit,
            offset,
        )
        total = await self._segment_repository.count_members(segment_id)

        return GetSegmentMembersResult(
            segment=definition,
            members=members,
            total=total,
            limit=limit,
            offset=offset,
        )
