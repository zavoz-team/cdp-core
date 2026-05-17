import json
from datetime import datetime
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from domain.event import EventType
from domain.identity import CustomerIdentifiers
from domain.profile import Currency, CustomerProfile, RecentEvent
from domain.segment import SegmentId
from usecase.criteria import ProfileListCriteria
from usecase.error import UseCaseDependencyError

_PROFILE_SELECT = """
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
            jsonb_agg(sm.segment_id),
            CAST('[]' AS jsonb)
         ) FROM segment_memberships sm
           WHERE sm.customer_id = cp.customer_id) AS segment_ids_json
    FROM customer_profiles cp
"""

_UPSERT_SQL = """
    INSERT INTO customer_profiles (
        customer_id,
        attributes_json,
        first_seen_at,
        last_seen_at,
        last_purchase_at,
        events_count,
        page_views_count,
        cart_adds_count,
        orders_count,
        total_revenue,
        currency,
        recent_events_json,
        created_at,
        updated_at
    ) VALUES (
        :customer_id,
        CAST(:attributes_json AS jsonb),
        :first_seen_at,
        :last_seen_at,
        :last_purchase_at,
        :events_count,
        :page_views_count,
        :cart_adds_count,
        :orders_count,
        :total_revenue,
        :currency,
        CAST(:recent_events_json AS jsonb),
        :created_at,
        :updated_at
    )
    ON CONFLICT (customer_id) DO UPDATE SET
        first_seen_at      = LEAST(customer_profiles.first_seen_at, EXCLUDED.first_seen_at),
        last_seen_at       = GREATEST(customer_profiles.last_seen_at, EXCLUDED.last_seen_at),
        last_purchase_at   = CASE
                                 WHEN EXCLUDED.last_purchase_at IS NOT NULL
                                 THEN GREATEST(customer_profiles.last_purchase_at, EXCLUDED.last_purchase_at)
                                 ELSE customer_profiles.last_purchase_at
                             END,
        events_count       = EXCLUDED.events_count,
        page_views_count   = EXCLUDED.page_views_count,
        cart_adds_count    = EXCLUDED.cart_adds_count,
        orders_count       = EXCLUDED.orders_count,
        total_revenue      = EXCLUDED.total_revenue,
        currency           = EXCLUDED.currency,
        attributes_json    = EXCLUDED.attributes_json,
        recent_events_json = EXCLUDED.recent_events_json,
        updated_at         = EXCLUDED.updated_at
"""


class CustomerProfileRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_customer_id(self, customer_id: str) -> CustomerProfile | None:
        try:
            result = await self._session.execute(
                sa.text(_PROFILE_SELECT + 'WHERE cp.customer_id = :customer_id'),
                {'customer_id': customer_id},
            )
        except Exception as exc:
            raise UseCaseDependencyError('customer profile lookup failed') from exc

        row = result.mappings().first()
        if row is None:
            return None
        return _row_to_profile(row)

    async def get_many_by_customer_ids(
        self,
        customer_ids: tuple[str, ...],
    ) -> tuple[CustomerProfile, ...]:
        if not customer_ids:
            return ()

        placeholders = ', '.join(f':cid_{i}' for i in range(len(customer_ids)))
        sql = _PROFILE_SELECT + f'WHERE cp.customer_id IN ({placeholders})'
        params = {f'cid_{i}': cid for i, cid in enumerate(customer_ids)}

        try:
            result = await self._session.execute(sa.text(sql), params)
        except Exception as exc:
            raise UseCaseDependencyError('customer profiles lookup failed') from exc

        return tuple(_row_to_profile(row) for row in result.mappings().all())

    async def list_profiles(
        self,
        criteria: ProfileListCriteria,
    ) -> tuple[CustomerProfile, ...]:
        filter_joins, filter_params = _filter_joins(criteria)
        sql = (
            _PROFILE_SELECT
            + filter_joins
            + '\nORDER BY cp.created_at DESC\nLIMIT :limit OFFSET :offset'
        )
        params: dict[str, Any] = {
            **filter_params,
            'limit': criteria.limit,
            'offset': criteria.offset,
        }

        try:
            result = await self._session.execute(sa.text(sql), params)
        except Exception as exc:
            raise UseCaseDependencyError('customer profiles list failed') from exc

        return tuple(_row_to_profile(row) for row in result.mappings().all())

    async def count_profiles(self, criteria: ProfileListCriteria) -> int:
        filter_joins, filter_params = _filter_joins(criteria)
        sql = (
            'SELECT COUNT(DISTINCT cp.customer_id)\n'
            'FROM customer_profiles cp\n'
            + filter_joins
        )

        try:
            result = await self._session.execute(sa.text(sql), filter_params)
        except Exception as exc:
            raise UseCaseDependencyError('customer profiles count failed') from exc

        return result.scalar() or 0

    async def save_profile(self, profile: CustomerProfile) -> None:
        try:
            await self._session.execute(
                sa.text(_UPSERT_SQL),
                _profile_to_params(profile),
            )
        except Exception as exc:
            raise UseCaseDependencyError('customer profile save failed') from exc


def _filter_joins(criteria: ProfileListCriteria) -> tuple[str, dict[str, Any]]:
    joins: list[str] = []
    params: dict[str, Any] = {}

    if criteria.segment_id is not None:
        joins.append(
            'INNER JOIN segment_memberships sm_f'
            ' ON sm_f.customer_id = cp.customer_id'
            ' AND sm_f.segment_id = :filter_segment_id'
        )
        params['filter_segment_id'] = criteria.segment_id.value

    if criteria.email is not None:
        joins.append(
            "INNER JOIN identity_links il_e"
            " ON il_e.customer_id = cp.customer_id"
            " AND il_e.identity_type = 'email'"
            " AND il_e.identity_value = :filter_email"
        )
        params['filter_email'] = criteria.email

    if criteria.phone is not None:
        joins.append(
            "INNER JOIN identity_links il_p"
            " ON il_p.customer_id = cp.customer_id"
            " AND il_p.identity_type = 'phone'"
            " AND il_p.identity_value = :filter_phone"
        )
        params['filter_phone'] = criteria.phone

    if criteria.external_user_id is not None:
        joins.append(
            "INNER JOIN identity_links il_x"
            " ON il_x.customer_id = cp.customer_id"
            " AND il_x.identity_type = 'external_user_id'"
            " AND il_x.identity_value = :filter_external_user_id"
        )
        params['filter_external_user_id'] = criteria.external_user_id

    return '\n'.join(joins), params


def _profile_to_params(profile: CustomerProfile) -> dict[str, Any]:
    return {
        'customer_id': profile.customer_id,
        'attributes_json': json.dumps(dict(profile.attributes)),
        'first_seen_at': profile.first_seen_at,
        'last_seen_at': profile.last_seen_at,
        'last_purchase_at': profile.last_purchase_at,
        'events_count': profile.events_count,
        'page_views_count': profile.page_views_count,
        'cart_adds_count': profile.cart_adds_count,
        'orders_count': profile.orders_count,
        'total_revenue': str(profile.total_revenue),
        'currency': profile.currency.value,
        'recent_events_json': json.dumps(_recent_events_to_list(profile.recent_events)),
        'created_at': profile.created_at,
        'updated_at': profile.updated_at,
    }


def _recent_events_to_list(events: tuple[RecentEvent, ...]) -> list[dict[str, Any]]:
    return [
        {
            'event_id': e.event_id,
            'event_type': e.event_type.value,
            'occurred_at': e.occurred_at.isoformat(),
        }
        for e in events
    ]


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
