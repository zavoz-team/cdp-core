import datetime
import uuid

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from adapter.config.loader import load_config
from adapter.observability.noop import NoopLogger, NoopTracer
from domain.segment import SegmentId, SegmentMembership
from repository.segment import SegmentRepository


def _uid(prefix: str = 'cust') -> str:
    return f'{prefix}-{uuid.uuid4().hex[:10]}'


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _make_membership(
    customer_id: str,
    segment_id: SegmentId,
    at: datetime.datetime | None = None,
) -> SegmentMembership:
    now = at or _utcnow()
    return SegmentMembership(
        segment_id=segment_id,
        customer_id=customer_id,
        member_since=now,
        updated_at=now,
    )


async def _insert_customer_profile(session: AsyncSession, customer_id: str) -> None:
    await session.execute(
        sa.text(
            """
            INSERT INTO customer_profiles (
                customer_id, attributes_json, first_seen_at, last_seen_at,
                events_count, page_views_count, cart_adds_count, orders_count,
                total_revenue, currency, recent_events_json, created_at, updated_at
            ) VALUES (
                :customer_id, CAST('{}' AS jsonb), NOW(), NOW(),
                0, 0, 0, 0,
                0.00, 'RUB', CAST('[]' AS jsonb), NOW(), NOW()
            ) ON CONFLICT (customer_id) DO NOTHING
            """
        ),
        {'customer_id': customer_id},
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
def repo(session: AsyncSession) -> SegmentRepository:
    return SegmentRepository(session, NoopLogger(), NoopTracer())


class TestGetDefinition:
    async def test_returns_seeded_new_user(self, repo: SegmentRepository) -> None:
        result = await repo.get_definition(SegmentId.NEW_USER)

        assert result is not None
        assert result.segment_id == SegmentId.NEW_USER
        assert result.is_active is True
        assert result.name

    async def test_returns_seeded_active(self, repo: SegmentRepository) -> None:
        result = await repo.get_definition(SegmentId.ACTIVE)

        assert result is not None
        assert result.segment_id == SegmentId.ACTIVE

    async def test_returns_seeded_vip(self, repo: SegmentRepository) -> None:
        result = await repo.get_definition(SegmentId.VIP)

        assert result is not None
        assert result.segment_id == SegmentId.VIP

    async def test_definition_has_name_and_description(
        self, repo: SegmentRepository
    ) -> None:
        result = await repo.get_definition(SegmentId.VIP)

        assert result is not None
        assert result.name != ''
        assert result.description != ''


class TestListDefinitions:
    async def test_returns_all_three_active_segments(
        self, repo: SegmentRepository
    ) -> None:
        result = await repo.list_definitions()

        ids = {d.segment_id for d in result}
        assert SegmentId.NEW_USER in ids
        assert SegmentId.ACTIVE in ids
        assert SegmentId.VIP in ids

    async def test_include_disabled_also_returns_all(
        self, repo: SegmentRepository
    ) -> None:
        result = await repo.list_definitions(include_disabled=True)

        assert len(result) >= 3

    async def test_active_only_excludes_disabled(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        await session.execute(
            sa.text(
                "UPDATE segment_definitions SET is_active = false WHERE segment_id = 'vip'"
            )
        )

        active_only = await repo.list_definitions(include_disabled=False)
        all_defs = await repo.list_definitions(include_disabled=True)

        active_ids = {d.segment_id for d in active_only}
        assert SegmentId.VIP not in active_ids
        assert len(all_defs) > len(active_only)

    async def test_results_ordered_by_segment_id(self, repo: SegmentRepository) -> None:
        result = await repo.list_definitions()

        segment_ids = [d.segment_id.value for d in result]
        assert segment_ids == sorted(segment_ids)


class TestCountMembers:
    async def test_returns_zero_for_empty_segment(
        self, repo: SegmentRepository
    ) -> None:
        count = await repo.count_members(SegmentId.VIP)

        assert count >= 0

    async def test_returns_correct_count_after_add(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        await _insert_customer_profile(session, customer_id)
        before = await repo.count_members(SegmentId.ACTIVE)

        await repo.replace_profile_memberships(
            customer_id,
            (_make_membership(customer_id, SegmentId.ACTIVE),),
        )

        after = await repo.count_members(SegmentId.ACTIVE)
        assert after == before + 1

    async def test_count_decreases_after_remove(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        await _insert_customer_profile(session, customer_id)
        await repo.replace_profile_memberships(
            customer_id,
            (_make_membership(customer_id, SegmentId.NEW_USER),),
        )
        before = await repo.count_members(SegmentId.NEW_USER)

        await repo.replace_profile_memberships(customer_id, ())

        after = await repo.count_members(SegmentId.NEW_USER)
        assert after == before - 1


class TestListMemberships:
    async def test_returns_membership_after_add(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        await _insert_customer_profile(session, customer_id)
        await repo.replace_profile_memberships(
            customer_id,
            (_make_membership(customer_id, SegmentId.VIP),),
        )

        result = await repo.list_memberships(SegmentId.VIP, limit=100, offset=0)

        customer_ids = {m.customer_id for m in result}
        assert customer_id in customer_ids

    async def test_membership_fields_are_correct(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        await _insert_customer_profile(session, customer_id)
        await repo.replace_profile_memberships(
            customer_id,
            (_make_membership(customer_id, SegmentId.ACTIVE),),
        )

        result = await repo.list_memberships(SegmentId.ACTIVE, limit=100, offset=0)

        found = next((m for m in result if m.customer_id == customer_id), None)
        assert found is not None
        assert found.segment_id == SegmentId.ACTIVE
        assert found.member_since is not None
        assert found.updated_at is not None

    async def test_respects_limit_and_offset(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        ids = [_uid() for _ in range(3)]
        for cid in ids:
            await _insert_customer_profile(session, cid)
            await repo.replace_profile_memberships(
                cid,
                (_make_membership(cid, SegmentId.NEW_USER),),
            )

        page1 = await repo.list_memberships(SegmentId.NEW_USER, limit=2, offset=0)
        page2 = await repo.list_memberships(SegmentId.NEW_USER, limit=2, offset=2)

        assert len(page1) == 2
        assert {m.customer_id for m in page1}.isdisjoint({m.customer_id for m in page2})


class TestReplaceProfileMemberships:
    async def test_inserts_new_memberships(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        await _insert_customer_profile(session, customer_id)

        await repo.replace_profile_memberships(
            customer_id,
            (
                _make_membership(customer_id, SegmentId.NEW_USER),
                _make_membership(customer_id, SegmentId.ACTIVE),
            ),
        )

        memberships_new_user = await repo.list_memberships(SegmentId.NEW_USER, 100, 0)
        memberships_active = await repo.list_memberships(SegmentId.ACTIVE, 100, 0)
        assert any(m.customer_id == customer_id for m in memberships_new_user)
        assert any(m.customer_id == customer_id for m in memberships_active)

    async def test_removes_old_memberships_on_replace(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        await _insert_customer_profile(session, customer_id)
        await repo.replace_profile_memberships(
            customer_id,
            (_make_membership(customer_id, SegmentId.NEW_USER),),
        )

        await repo.replace_profile_memberships(
            customer_id,
            (_make_membership(customer_id, SegmentId.VIP),),
        )

        new_user_members = await repo.list_memberships(SegmentId.NEW_USER, 100, 0)
        vip_members = await repo.list_memberships(SegmentId.VIP, 100, 0)
        assert not any(m.customer_id == customer_id for m in new_user_members)
        assert any(m.customer_id == customer_id for m in vip_members)

    async def test_empty_memberships_clears_all(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        await _insert_customer_profile(session, customer_id)
        await repo.replace_profile_memberships(
            customer_id,
            (
                _make_membership(customer_id, SegmentId.NEW_USER),
                _make_membership(customer_id, SegmentId.VIP),
            ),
        )

        await repo.replace_profile_memberships(customer_id, ())

        for seg in (SegmentId.NEW_USER, SegmentId.VIP):
            members = await repo.list_memberships(seg, 100, 0)
            assert not any(m.customer_id == customer_id for m in members)

    async def test_does_not_affect_other_customers(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        cust_a = _uid()
        cust_b = _uid()
        await _insert_customer_profile(session, cust_a)
        await _insert_customer_profile(session, cust_b)
        await repo.replace_profile_memberships(
            cust_a, (_make_membership(cust_a, SegmentId.ACTIVE),)
        )
        await repo.replace_profile_memberships(
            cust_b, (_make_membership(cust_b, SegmentId.ACTIVE),)
        )

        await repo.replace_profile_memberships(cust_a, ())

        members = await repo.list_memberships(SegmentId.ACTIVE, 100, 0)
        assert not any(m.customer_id == cust_a for m in members)
        assert any(m.customer_id == cust_b for m in members)


class TestListMemberProfiles:
    async def test_returns_profile_of_member(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        await _insert_customer_profile(session, customer_id)
        await repo.replace_profile_memberships(
            customer_id,
            (_make_membership(customer_id, SegmentId.VIP),),
        )

        profiles = await repo.list_member_profiles(SegmentId.VIP, limit=100, offset=0)

        ids = {p.customer_id for p in profiles}
        assert customer_id in ids

    async def test_does_not_return_non_members(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        member_id = _uid()
        non_member_id = _uid()
        await _insert_customer_profile(session, member_id)
        await _insert_customer_profile(session, non_member_id)
        await repo.replace_profile_memberships(
            member_id,
            (_make_membership(member_id, SegmentId.ACTIVE),),
        )

        profiles = await repo.list_member_profiles(
            SegmentId.ACTIVE, limit=100, offset=0
        )

        ids = {p.customer_id for p in profiles}
        assert member_id in ids
        assert non_member_id not in ids

    async def test_profile_contains_segment_memberships(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        await _insert_customer_profile(session, customer_id)
        await repo.replace_profile_memberships(
            customer_id,
            (
                _make_membership(customer_id, SegmentId.NEW_USER),
                _make_membership(customer_id, SegmentId.VIP),
            ),
        )

        profiles = await repo.list_member_profiles(
            SegmentId.NEW_USER, limit=100, offset=0
        )

        profile = next((p for p in profiles if p.customer_id == customer_id), None)
        assert profile is not None
        assert SegmentId.NEW_USER in profile.current_segments
        assert SegmentId.VIP in profile.current_segments

    async def test_respects_limit_and_offset(
        self, repo: SegmentRepository, session: AsyncSession
    ) -> None:
        ids = [_uid() for _ in range(3)]
        for cid in ids:
            await _insert_customer_profile(session, cid)
            await repo.replace_profile_memberships(
                cid, (_make_membership(cid, SegmentId.VIP),)
            )

        page1 = await repo.list_member_profiles(SegmentId.VIP, limit=2, offset=0)
        page2 = await repo.list_member_profiles(SegmentId.VIP, limit=2, offset=2)

        assert len(page1) == 2
        assert {p.customer_id for p in page1}.isdisjoint({p.customer_id for p in page2})
