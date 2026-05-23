from datetime import datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from adapter.fastapi.dependencies import provide
from domain.profile import CustomerProfile
from domain.segment import SegmentId
from usecase.criteria import ProfileListCriteria
from usecase.error import ProfileNotFoundUseCaseError
from usecase.profile import ProfileService

router = APIRouter(prefix='/profiles', tags=['Profiles'])


class IdentifiersResponse(BaseModel):
    emails: list[str]
    phones: list[str]
    external_user_ids: list[str]


class CountersResponse(BaseModel):
    events_count: int
    page_views_count: int
    cart_adds_count: int
    orders_count: int


class TimestampsResponse(BaseModel):
    first_seen_at: datetime
    last_seen_at: datetime
    created_at: datetime
    updated_at: datetime
    last_purchase_at: datetime | None


class RecentEventResponse(BaseModel):
    event_id: str
    event_type: str
    occurred_at: datetime


class ProfileDetailsResponse(BaseModel):
    customer_id: str
    identifiers: IdentifiersResponse
    attributes: dict[str, Any]
    counters: CountersResponse
    total_revenue: Decimal
    currency: str
    timestamps: TimestampsResponse
    segments: list[str]
    recent_events: list[RecentEventResponse]


class ProfileListItemResponse(BaseModel):
    customer_id: str
    email: str | None
    phone: str | None
    external_user_id: str | None
    segments: list[str]
    total_orders: int
    total_revenue: Decimal
    last_seen_at: datetime | None


class ProfileListResponse(BaseModel):
    items: list[ProfileListItemResponse]
    total: int
    limit: int
    offset: int


def _map_profile_details(profile: CustomerProfile) -> ProfileDetailsResponse:
    return ProfileDetailsResponse(
        customer_id=profile.customer_id,
        identifiers=IdentifiersResponse(
            emails=list(profile.identifiers.emails),
            phones=list(profile.identifiers.phones),
            external_user_ids=list(profile.identifiers.external_user_ids),
        ),
        attributes=dict(profile.attributes),
        counters=CountersResponse(
            events_count=profile.events_count,
            page_views_count=profile.page_views_count,
            cart_adds_count=profile.cart_adds_count,
            orders_count=profile.orders_count,
        ),
        total_revenue=profile.total_revenue,
        currency=profile.currency,
        timestamps=TimestampsResponse(
            first_seen_at=profile.first_seen_at,
            last_seen_at=profile.last_seen_at,
            created_at=profile.created_at,
            updated_at=profile.updated_at,
            last_purchase_at=profile.last_purchase_at,
        ),
        segments=[str(s) for s in profile.current_segments],
        recent_events=[
            RecentEventResponse(
                event_id=ev.event_id,
                event_type=str(ev.event_type),
                occurred_at=ev.occurred_at,
            )
            for ev in profile.recent_events
        ],
    )


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


@router.get('', response_model=ProfileListResponse)
async def list_profiles(
    limit: int = 50,
    offset: int = 0,
    segment_id: str | None = None,
    email: str | None = None,
    phone: str | None = None,
    external_user_id: str | None = None,
    service: ProfileService = provide(ProfileService),
) -> ProfileListResponse:
    criteria = ProfileListCriteria(
        limit=limit,
        offset=offset,
        segment_id=SegmentId(segment_id) if segment_id else None,
        email=email,
        phone=phone,
        external_user_id=external_user_id,
    )
    result = await service.list_profiles(criteria)

    return ProfileListResponse(
        items=[_map_profile_list_item(p) for p in result.profiles],
        total=result.total,
        limit=result.limit,
        offset=result.offset,
    )


@router.get('/{customer_id}', response_model=ProfileDetailsResponse)
async def get_profile(
    customer_id: str,
    service: ProfileService = provide(ProfileService),
) -> ProfileDetailsResponse:
    try:
        result = await service.get_profile(customer_id)
        return _map_profile_details(result.profile)
    except ProfileNotFoundUseCaseError:
        raise HTTPException(status_code=404, detail='Profile not found')
