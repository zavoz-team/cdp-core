from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from adapter.fastapi.dependencies import provide
from domain.profile import CustomerProfile
from domain.segment import SegmentId
from usecase.error import SegmentNotFoundUseCaseError
from usecase.segment import SegmentService

router = APIRouter(prefix='/segments', tags=['Segments'])


class SegmentSummaryResponse(BaseModel):
    segment_id: str
    name: str
    description: str
    is_active: bool
    members_count: int


class SegmentListResponse(BaseModel):
    items: list[SegmentSummaryResponse]


class ProfileListItemResponse(BaseModel):
    customer_id: str
    email: str | None
    phone: str | None
    external_user_id: str | None
    segments: list[str]
    total_orders: int
    total_revenue: Decimal
    last_seen_at: datetime | None


class SegmentMembersResponse(BaseModel):
    segment_id: str
    items: list[ProfileListItemResponse]
    total: int
    limit: int
    offset: int


def _map_profile_list_item(profile: CustomerProfile) -> ProfileListItemResponse:
    return ProfileListItemResponse(
        customer_id=profile.customer_id,
        email=profile.primary_email,
        phone=profile.primary_phone,
        external_user_id=profile.primary_external_user_id,
        segments=[str(s) for s in profile.current_segments],
        total_orders=profile.orders_count,
        total_revenue=profile.total_revenue,
        last_seen_at=profile.last_seen_at,
    )


@router.get('', response_model=SegmentListResponse)
async def list_segments(
    service: SegmentService = provide(SegmentService),
) -> SegmentListResponse:
    result = await service.list_segments()
    return SegmentListResponse(
        items=[
            SegmentSummaryResponse(
                segment_id=str(s.definition.segment_id),
                name=s.definition.name,
                description=s.definition.description,
                is_active=s.definition.is_active,
                members_count=s.members_count,
            )
            for s in result.segments
        ]
    )


@router.get('/{segment_id}/members', response_model=SegmentMembersResponse)
async def get_segment_members(
    segment_id: str,
    limit: int = 50,
    offset: int = 0,
    service: SegmentService = provide(SegmentService),
) -> SegmentMembersResponse:
    try:
        sid = SegmentId(segment_id)
    except ValueError:
        raise HTTPException(status_code=404, detail='Segment not found')

    try:
        result = await service.get_segment_members(sid, limit=limit, offset=offset)
        return SegmentMembersResponse(
            segment_id=str(result.segment.segment_id),
            items=[_map_profile_list_item(p) for p in result.members],
            total=result.total,
            limit=result.limit,
            offset=result.offset,
        )
    except SegmentNotFoundUseCaseError:
        raise HTTPException(status_code=404, detail='Segment not found')
