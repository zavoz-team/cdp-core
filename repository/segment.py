import json
from datetime import datetime
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from domain.event import EventType
from domain.identity import CustomerIdentifiers
from domain.profile import Currency, CustomerProfile, RecentEvent
from domain.segment import SegmentDefinition, SegmentId, SegmentMembership
from usecase.error import UseCaseDependencyError

_MEMBER_PROFILE_SQL = """
    SELECT
        cp.customer_id,
        cp.attributes_json,
        cp.first_seen_at,
        cp.last_seen_at,
        cp.last_purchase_at,
        cp.events_count,
        cp.page_views_count,
        cp.cart_adds_count,
        cp.orders_count,
        cp.total_revenue,
        cp.currency,
        cp.recent_events_json,
        cp.created_at,
        cp.updated_at,
        (SELECT COALESCE(
            jsonb_agg(jsonb_build_object('type', il.identity_type, 'value', il.identity_value)),
            CAST('[]' AS jsonb)
         ) FROM identity_links il
           WHERE il.customer_id = cp.customer_id) AS identity_links_json,
        (SELECT COALESCE(
            jsonb_agg(sm_all.segment_id),
            CAST('[]' AS jsonb)
         ) FROM segment_memberships sm_all
           WHERE sm_all.customer_id = cp.customer_id) AS segment_ids_json
    FROM customer_profiles cp
    INNER JOIN segment_memberships sm_filter
        ON sm_filter.customer_id = cp.customer_id
        AND sm_filter.segment_id = :segment_id
    ORDER BY cp.created_at DESC
    LIMIT :limit OFFSET :offset
"""


class SegmentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_definition(self, segment_id: SegmentId) -> SegmentDefinition | None:
        try:
            result = await self._session.execute(
                sa.text(
                    """
                    SELECT segment_id, name, description, is_active
                    FROM segment_definitions
                    WHERE segment_id = :segment_id
                    """
                ),
                {'segment_id': segment_id.value},
            )
        except Exception as exc:
            raise UseCaseDependencyError('segment definition lookup failed') from exc

        row = result.mappings().first()
        if row is None:
            return None
        return _row_to_definition(row)

    async def list_definitions(
        self,
        include_disabled: bool = False,
    ) -> tuple[SegmentDefinition, ...]:
        where = '' if include_disabled else 'WHERE is_active = true'
        sql = f"""
            SELECT segment_id, name, description, is_active
            FROM segment_definitions
            {where}
            ORDER BY segment_id
        """
        try:
            result = await self._session.execute(sa.text(sql))
        except Exception as exc:
            raise UseCaseDependencyError('segment definitions list failed') from exc

        return tuple(_row_to_definition(row) for row in result.mappings().all())

    async def count_members(self, segment_id: SegmentId) -> int:
        try:
            result = await self._session.execute(
                sa.text(
                    """
                    SELECT COUNT(*)
                    FROM segment_memberships
                    WHERE segment_id = :segment_id
                    """
                ),
                {'segment_id': segment_id.value},
            )
        except Exception as exc:
            raise UseCaseDependencyError('segment member count failed') from exc

        return result.scalar() or 0

    async def list_memberships(
        self,
        segment_id: SegmentId,
        limit: int,
        offset: int,
    ) -> tuple[SegmentMembership, ...]:
        try:
            result = await self._session.execute(
                sa.text(
                    """
                    SELECT segment_id, customer_id, matched_at
                    FROM segment_memberships
                    WHERE segment_id = :segment_id
                    ORDER BY matched_at DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {'segment_id': segment_id.value, 'limit': limit, 'offset': offset},
            )
        except Exception as exc:
            raise UseCaseDependencyError('segment memberships list failed') from exc

        return tuple(_row_to_membership(row) for row in result.mappings().all())

    async def list_member_profiles(
        self,
        segment_id: SegmentId,
        limit: int,
        offset: int,
    ) -> tuple[CustomerProfile, ...]:
        try:
            result = await self._session.execute(
                sa.text(_MEMBER_PROFILE_SQL),
                {'segment_id': segment_id.value, 'limit': limit, 'offset': offset},
            )
        except Exception as exc:
            raise UseCaseDependencyError('segment member profiles list failed') from exc

        return tuple(_row_to_profile(row) for row in result.mappings().all())

    async def replace_profile_memberships(
        self,
        customer_id: str,
        memberships: tuple[SegmentMembership, ...],
    ) -> None:
        try:
            await self._session.execute(
                sa.text(
                    'DELETE FROM segment_memberships WHERE customer_id = :customer_id'
                ),
                {'customer_id': customer_id},
            )
            if memberships:
                await self._session.execute(
                    sa.text(
                        """
                        INSERT INTO segment_memberships (segment_id, customer_id, matched_at)
                        VALUES (:segment_id, :customer_id, :matched_at)
                        ON CONFLICT (segment_id, customer_id) DO NOTHING
                        """
                    ),
                    [
                        {
                            'segment_id': m.segment_id.value,
                            'customer_id': m.customer_id,
                            'matched_at': m.member_since,
                        }
                        for m in memberships
                    ],
                )
        except Exception as exc:
            raise UseCaseDependencyError('segment membership replace failed') from exc


def _row_to_definition(row: Any) -> SegmentDefinition:
    return SegmentDefinition(
        segment_id=SegmentId(row['segment_id']),
        name=row['name'],
        description=row['description'] or '',
        is_active=row['is_active'],
    )


def _row_to_membership(row: Any) -> SegmentMembership:
    return SegmentMembership(
        segment_id=SegmentId(row['segment_id']),
        customer_id=row['customer_id'],
        member_since=row['matched_at'],
        updated_at=row['matched_at'],
    )


def _parse_json_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value)
    return list(value)


def _parse_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _build_identifiers(links: list) -> CustomerIdentifiers:
    emails: list[str] = []
    phones: list[str] = []
    external_user_ids: list[str] = []
    for link in links:
        itype = link['type']
        value = link['value']
        if itype == 'email':
            emails.append(value)
        elif itype == 'phone':
            phones.append(value)
        elif itype == 'external_user_id':
            external_user_ids.append(value)
    return CustomerIdentifiers(
        emails=tuple(emails),
        phones=tuple(phones),
        external_user_ids=tuple(external_user_ids),
    )


def _build_segments(segment_ids: list) -> frozenset[SegmentId]:
    result: set[SegmentId] = set()
    for sid in segment_ids:
        try:
            result.add(SegmentId(sid))
        except ValueError:
            pass
    return frozenset(result)


def _build_recent_events(data: list) -> tuple[RecentEvent, ...]:
    result: list[RecentEvent] = []
    for item in data:
        try:
            result.append(
                RecentEvent(
                    event_id=item['event_id'],
                    event_type=EventType(item['event_type']),
                    occurred_at=datetime.fromisoformat(item['occurred_at']),
                )
            )
        except (KeyError, ValueError):
            pass
    return tuple(result)


def _row_to_profile(row: Any) -> CustomerProfile:
    return CustomerProfile(
        customer_id=row['customer_id'],
        attributes=_parse_json_dict(row['attributes_json']),
        first_seen_at=row['first_seen_at'],
        last_seen_at=row['last_seen_at'],
        last_purchase_at=row['last_purchase_at'],
        events_count=row['events_count'],
        page_views_count=row['page_views_count'],
        cart_adds_count=row['cart_adds_count'],
        orders_count=row['orders_count'],
        total_revenue=Decimal(str(row['total_revenue'])),
        currency=Currency(row['currency']),
        recent_events=_build_recent_events(_parse_json_list(row['recent_events_json'])),
        created_at=row['created_at'],
        updated_at=row['updated_at'],
        identifiers=_build_identifiers(_parse_json_list(row['identity_links_json'])),
        current_segments=_build_segments(_parse_json_list(row['segment_ids_json'])),
    )
