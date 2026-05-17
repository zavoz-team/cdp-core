import datetime
import uuid
from decimal import Decimal

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from adapter.config.loader import load_config
from domain.event import EventType
from domain.identity import CustomerIdentifiers
from domain.profile import Currency, CustomerProfile, RecentEvent
from domain.segment import SegmentId
from repository.customer_profile import CustomerProfileRepository
from usecase.criteria import ProfileListCriteria


def _uid(prefix: str = 'cust') -> str:
    return f'{prefix}-{uuid.uuid4().hex[:10]}'


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _make_profile(
    customer_id: str | None = None,
    first_seen_at: datetime.datetime | None = None,
    last_seen_at: datetime.datetime | None = None,
    last_purchase_at: datetime.datetime | None = None,
    events_count: int = 0,
    page_views_count: int = 0,
    cart_adds_count: int = 0,
    orders_count: int = 0,
    total_revenue: str = '0',
    attributes: dict | None = None,
    recent_events: tuple[RecentEvent, ...] = (),
) -> CustomerProfile:
    now = _utcnow()
    return CustomerProfile(
        customer_id=customer_id or _uid(),
        first_seen_at=first_seen_at or now,
        last_seen_at=last_seen_at or now,
        last_purchase_at=last_purchase_at,
        events_count=events_count,
        page_views_count=page_views_count,
        cart_adds_count=cart_adds_count,
        orders_count=orders_count,
        total_revenue=Decimal(total_revenue),
        attributes=attributes or {},
        recent_events=recent_events,
        created_at=now,
        updated_at=now,
    )


async def _insert_identity_link(
    session: AsyncSession,
    customer_id: str,
    identity_type: str,
    identity_value: str,
) -> None:
    await session.execute(
        sa.text(
            """
            INSERT INTO identity_links (identity_type, identity_value, customer_id, created_at)
            VALUES (:identity_type, :identity_value, :customer_id, :created_at)
            ON CONFLICT (identity_type, identity_value) DO NOTHING
            """
        ),
        {
            'identity_type': identity_type,
            'identity_value': identity_value,
            'customer_id': customer_id,
            'created_at': _utcnow(),
        },
    )


async def _insert_segment_membership(
    session: AsyncSession,
    customer_id: str,
    segment_id: str,
) -> None:
    await session.execute(
        sa.text(
            """
            INSERT INTO segment_memberships (segment_id, customer_id, matched_at)
            VALUES (:segment_id, :customer_id, :matched_at)
            ON CONFLICT (segment_id, customer_id) DO NOTHING
            """
        ),
        {
            'segment_id': segment_id,
            'customer_id': customer_id,
            'matched_at': _utcnow(),
        },
    )


@pytest_asyncio.fixture
async def engine():
    cfg = load_config()
    pg = cfg.postgres
    url = (
        f'postgresql+asyncpg://{pg.user}:{pg.password}'
        f'@{pg.host}:{pg.port}/{pg.database}'
    )
    eng = create_async_engine(url, echo=False)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine):
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        yield s
        await s.rollback()


@pytest.fixture
def repo(session: AsyncSession) -> CustomerProfileRepository:
    return CustomerProfileRepository(session)


class TestGetByCustomerId:
    async def test_returns_none_for_missing(self, repo: CustomerProfileRepository) -> None:
        result = await repo.get_by_customer_id('does-not-exist')

        assert result is None

    async def test_returns_profile_after_save(self, repo: CustomerProfileRepository) -> None:
        profile = _make_profile()
        await repo.save_profile(profile)

        found = await repo.get_by_customer_id(profile.customer_id)

        assert found is not None
        assert found.customer_id == profile.customer_id

    async def test_round_trips_counters(self, repo: CustomerProfileRepository) -> None:
        profile = _make_profile(
            events_count=10,
            page_views_count=7,
            cart_adds_count=3,
            orders_count=2,
        )
        await repo.save_profile(profile)

        found = await repo.get_by_customer_id(profile.customer_id)

        assert found is not None
        assert found.events_count == 10
        assert found.page_views_count == 7
        assert found.cart_adds_count == 3
        assert found.orders_count == 2

    async def test_round_trips_total_revenue(self, repo: CustomerProfileRepository) -> None:
        profile = _make_profile(total_revenue='98765.43')
        await repo.save_profile(profile)

        found = await repo.get_by_customer_id(profile.customer_id)

        assert found is not None
        assert found.total_revenue == Decimal('98765.43')
        assert found.currency == Currency.RUB

    async def test_round_trips_attributes(self, repo: CustomerProfileRepository) -> None:
        profile = _make_profile(attributes={'city': 'Moscow', 'tier': 'gold'})
        await repo.save_profile(profile)

        found = await repo.get_by_customer_id(profile.customer_id)

        assert found is not None
        assert found.attributes['city'] == 'Moscow'
        assert found.attributes['tier'] == 'gold'

    async def test_round_trips_last_purchase_at(self, repo: CustomerProfileRepository) -> None:
        purchase_at = _utcnow() - datetime.timedelta(hours=2)
        profile = _make_profile(last_purchase_at=purchase_at)
        await repo.save_profile(profile)

        found = await repo.get_by_customer_id(profile.customer_id)

        assert found is not None
        assert found.last_purchase_at is not None

    async def test_last_purchase_at_none_when_not_set(self, repo: CustomerProfileRepository) -> None:
        profile = _make_profile(last_purchase_at=None)
        await repo.save_profile(profile)

        found = await repo.get_by_customer_id(profile.customer_id)

        assert found is not None
        assert found.last_purchase_at is None

    async def test_round_trips_recent_events(self, repo: CustomerProfileRepository) -> None:
        now = _utcnow()
        recent = (
            RecentEvent(event_id='ev-1', event_type=EventType.PAGE_VIEW, occurred_at=now),
        )
        profile = _make_profile(recent_events=recent)
        await repo.save_profile(profile)

        found = await repo.get_by_customer_id(profile.customer_id)

        assert found is not None
        assert len(found.recent_events) == 1
        assert found.recent_events[0].event_id == 'ev-1'
        assert found.recent_events[0].event_type == EventType.PAGE_VIEW

    async def test_loads_identity_links(
        self,
        repo: CustomerProfileRepository,
        session: AsyncSession,
    ) -> None:
        profile = _make_profile()
        await repo.save_profile(profile)
        await _insert_identity_link(session, profile.customer_id, 'email', 'id@example.com')
        await _insert_identity_link(session, profile.customer_id, 'phone', '+79991234567')

        found = await repo.get_by_customer_id(profile.customer_id)

        assert found is not None
        assert 'id@example.com' in found.identifiers.emails
        assert '+79991234567' in found.identifiers.phones

    async def test_loads_segment_memberships(
        self,
        repo: CustomerProfileRepository,
        session: AsyncSession,
    ) -> None:
        profile = _make_profile()
        await repo.save_profile(profile)
        await _insert_segment_membership(session, profile.customer_id, 'vip')

        found = await repo.get_by_customer_id(profile.customer_id)

        assert found is not None
        assert SegmentId.VIP in found.current_segments

    async def test_empty_identifiers_when_no_links(self, repo: CustomerProfileRepository) -> None:
        profile = _make_profile()
        await repo.save_profile(profile)

        found = await repo.get_by_customer_id(profile.customer_id)

        assert found is not None
        assert found.identifiers == CustomerIdentifiers()
        assert found.current_segments == frozenset()


class TestOutOfOrderDates:
    async def test_first_seen_at_uses_least(self, repo: CustomerProfileRepository) -> None:
        now = _utcnow()
        t_early = now - datetime.timedelta(days=10)
        t_late = now - datetime.timedelta(days=1)
        customer_id = _uid()

        first_save = _make_profile(
            customer_id=customer_id,
            first_seen_at=t_late,
            last_seen_at=t_late,
        )
        await repo.save_profile(first_save)

        second_save = _make_profile(
            customer_id=customer_id,
            first_seen_at=t_early,
            last_seen_at=t_late,
        )
        await repo.save_profile(second_save)

        found = await repo.get_by_customer_id(customer_id)
        assert found is not None
        assert found.first_seen_at.replace(microsecond=0) <= t_early.replace(microsecond=0) + datetime.timedelta(seconds=1)

    async def test_last_seen_at_uses_greatest(self, repo: CustomerProfileRepository) -> None:
        now = _utcnow()
        t_early = now - datetime.timedelta(days=5)
        t_late = now
        customer_id = _uid()

        first_save = _make_profile(
            customer_id=customer_id,
            first_seen_at=t_early,
            last_seen_at=t_early,
        )
        await repo.save_profile(first_save)

        second_save = _make_profile(
            customer_id=customer_id,
            first_seen_at=t_early,
            last_seen_at=t_late,
        )
        await repo.save_profile(second_save)

        found = await repo.get_by_customer_id(customer_id)
        assert found is not None
        assert found.last_seen_at >= t_late - datetime.timedelta(seconds=1)

    async def test_last_purchase_at_uses_greatest(self, repo: CustomerProfileRepository) -> None:
        now = _utcnow()
        t_old_purchase = now - datetime.timedelta(days=3)
        t_new_purchase = now - datetime.timedelta(days=1)
        customer_id = _uid()

        await repo.save_profile(_make_profile(
            customer_id=customer_id,
            last_purchase_at=t_new_purchase,
        ))
        await repo.save_profile(_make_profile(
            customer_id=customer_id,
            last_purchase_at=t_old_purchase,
        ))

        found = await repo.get_by_customer_id(customer_id)
        assert found is not None
        assert found.last_purchase_at is not None
        assert found.last_purchase_at >= t_new_purchase - datetime.timedelta(seconds=1)

    async def test_last_purchase_at_not_overwritten_by_none(
        self, repo: CustomerProfileRepository
    ) -> None:
        now = _utcnow()
        purchase_at = now - datetime.timedelta(hours=1)
        customer_id = _uid()

        await repo.save_profile(_make_profile(
            customer_id=customer_id,
            last_purchase_at=purchase_at,
        ))
        await repo.save_profile(_make_profile(
            customer_id=customer_id,
            last_purchase_at=None,
        ))

        found = await repo.get_by_customer_id(customer_id)
        assert found is not None
        assert found.last_purchase_at is not None

    async def test_created_at_never_overwritten(self, repo: CustomerProfileRepository) -> None:
        now = _utcnow()
        original_created_at = now - datetime.timedelta(days=30)
        customer_id = _uid()

        original = CustomerProfile(
            customer_id=customer_id,
            first_seen_at=original_created_at,
            last_seen_at=original_created_at,
            created_at=original_created_at,
            updated_at=original_created_at,
        )
        await repo.save_profile(original)

        updated = CustomerProfile(
            customer_id=customer_id,
            first_seen_at=now,
            last_seen_at=now,
            created_at=now,
            updated_at=now,
        )
        await repo.save_profile(updated)

        found = await repo.get_by_customer_id(customer_id)
        assert found is not None
        diff = abs((found.created_at - original_created_at).total_seconds())
        assert diff < 1


class TestGetManyByCustomerIds:
    async def test_returns_empty_for_empty_input(self, repo: CustomerProfileRepository) -> None:
        result = await repo.get_many_by_customer_ids(())

        assert result == ()

    async def test_returns_all_matching_profiles(self, repo: CustomerProfileRepository) -> None:
        p1 = _make_profile()
        p2 = _make_profile()
        await repo.save_profile(p1)
        await repo.save_profile(p2)

        result = await repo.get_many_by_customer_ids((p1.customer_id, p2.customer_id))

        ids = {p.customer_id for p in result}
        assert p1.customer_id in ids
        assert p2.customer_id in ids

    async def test_skips_missing_ids(self, repo: CustomerProfileRepository) -> None:
        profile = _make_profile()
        await repo.save_profile(profile)

        result = await repo.get_many_by_customer_ids((profile.customer_id, 'does-not-exist'))

        assert len(result) == 1
        assert result[0].customer_id == profile.customer_id


class TestListProfiles:
    async def test_returns_saved_profiles(self, repo: CustomerProfileRepository) -> None:
        p1 = _make_profile()
        p2 = _make_profile()
        await repo.save_profile(p1)
        await repo.save_profile(p2)

        result = await repo.list_profiles(ProfileListCriteria(limit=100, offset=0))

        ids = {p.customer_id for p in result}
        assert p1.customer_id in ids
        assert p2.customer_id in ids

    async def test_respects_limit_and_offset(self, repo: CustomerProfileRepository) -> None:
        for _ in range(3):
            await repo.save_profile(_make_profile())

        page1 = await repo.list_profiles(ProfileListCriteria(limit=2, offset=0))
        page2 = await repo.list_profiles(ProfileListCriteria(limit=2, offset=2))

        assert len(page1) == 2
        assert len(page2) <= 2
        assert {p.customer_id for p in page1}.isdisjoint({p.customer_id for p in page2})

    async def test_filters_by_segment_id(
        self,
        repo: CustomerProfileRepository,
        session: AsyncSession,
    ) -> None:
        in_segment = _make_profile()
        not_in_segment = _make_profile()
        await repo.save_profile(in_segment)
        await repo.save_profile(not_in_segment)
        await _insert_segment_membership(session, in_segment.customer_id, 'active')

        result = await repo.list_profiles(
            ProfileListCriteria(limit=100, offset=0, segment_id=SegmentId.ACTIVE)
        )

        ids = {p.customer_id for p in result}
        assert in_segment.customer_id in ids
        assert not_in_segment.customer_id not in ids

    async def test_filters_by_email(
        self,
        repo: CustomerProfileRepository,
        session: AsyncSession,
    ) -> None:
        target = _make_profile()
        other = _make_profile()
        await repo.save_profile(target)
        await repo.save_profile(other)
        await _insert_identity_link(session, target.customer_id, 'email', 'find@example.com')

        result = await repo.list_profiles(
            ProfileListCriteria(limit=100, offset=0, email='find@example.com')
        )

        ids = {p.customer_id for p in result}
        assert target.customer_id in ids
        assert other.customer_id not in ids

    async def test_filters_by_phone(
        self,
        repo: CustomerProfileRepository,
        session: AsyncSession,
    ) -> None:
        target = _make_profile()
        await repo.save_profile(target)
        await _insert_identity_link(session, target.customer_id, 'phone', '+70001112233')

        result = await repo.list_profiles(
            ProfileListCriteria(limit=100, offset=0, phone='+70001112233')
        )

        assert any(p.customer_id == target.customer_id for p in result)

    async def test_filters_by_external_user_id(
        self,
        repo: CustomerProfileRepository,
        session: AsyncSession,
    ) -> None:
        target = _make_profile()
        await repo.save_profile(target)
        await _insert_identity_link(session, target.customer_id, 'external_user_id', 'ext-abc')

        result = await repo.list_profiles(
            ProfileListCriteria(limit=100, offset=0, external_user_id='ext-abc')
        )

        assert any(p.customer_id == target.customer_id for p in result)


class TestCountProfiles:
    async def test_counts_all_profiles(self, repo: CustomerProfileRepository) -> None:
        before = await repo.count_profiles(ProfileListCriteria(limit=1000, offset=0))
        await repo.save_profile(_make_profile())
        await repo.save_profile(_make_profile())

        after = await repo.count_profiles(ProfileListCriteria(limit=1000, offset=0))

        assert after == before + 2

    async def test_counts_with_segment_filter(
        self,
        repo: CustomerProfileRepository,
        session: AsyncSession,
    ) -> None:
        profile = _make_profile()
        await repo.save_profile(profile)
        await _insert_segment_membership(session, profile.customer_id, 'new_user')

        count = await repo.count_profiles(
            ProfileListCriteria(limit=1000, offset=0, segment_id=SegmentId.NEW_USER)
        )

        assert count >= 1
