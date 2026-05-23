import uuid

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from adapter.config.loader import load_config
from domain.identity import IdentityLink, KnownIdentifier, KnownIdentifierType
from repository.identity import IdentityRepository


def _uid(prefix: str = 'cust') -> str:
    return f'{prefix}-{uuid.uuid4().hex[:10]}'


def _email(prefix: str = 'user') -> str:
    return f'{prefix}-{uuid.uuid4().hex[:6]}@example.com'


def _make_link(
    identity_type: KnownIdentifierType,
    identity_value: str,
    customer_id: str,
) -> IdentityLink:
    return IdentityLink(
        identity_type=identity_type,
        identity_value=identity_value,
        customer_id=customer_id,
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
def repo(session: AsyncSession) -> IdentityRepository:
    return IdentityRepository(session)


class TestGetLink:
    async def test_returns_none_for_missing(self, repo: IdentityRepository) -> None:
        identifier = KnownIdentifier(KnownIdentifierType.EMAIL, 'nobody@example.com')

        result = await repo.get_link(identifier)

        assert result is None

    async def test_returns_link_after_save(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        email = _email()
        await _insert_customer_profile(session, customer_id)
        link = _make_link(KnownIdentifierType.EMAIL, email, customer_id)
        await repo.save_links((link,))

        found = await repo.get_link(KnownIdentifier(KnownIdentifierType.EMAIL, email))

        assert found is not None
        assert found.identity_type == KnownIdentifierType.EMAIL
        assert found.identity_value == email
        assert found.customer_id == customer_id

    async def test_round_trips_phone_link(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        phone = f'+7999{uuid.uuid4().hex[:7]}'
        await _insert_customer_profile(session, customer_id)
        link = _make_link(KnownIdentifierType.PHONE, phone, customer_id)
        await repo.save_links((link,))

        found = await repo.get_link(KnownIdentifier(KnownIdentifierType.PHONE, phone))

        assert found is not None
        assert found.identity_type == KnownIdentifierType.PHONE
        assert found.identity_value == phone

    async def test_round_trips_external_user_id(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        ext_id = f'ext-{uuid.uuid4().hex[:8]}'
        await _insert_customer_profile(session, customer_id)
        link = _make_link(KnownIdentifierType.EXTERNAL_USER_ID, ext_id, customer_id)
        await repo.save_links((link,))

        found = await repo.get_link(
            KnownIdentifier(KnownIdentifierType.EXTERNAL_USER_ID, ext_id)
        )

        assert found is not None
        assert found.identity_type == KnownIdentifierType.EXTERNAL_USER_ID

    async def test_does_not_find_different_type(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        value = _email()
        await _insert_customer_profile(session, customer_id)
        link = _make_link(KnownIdentifierType.EMAIL, value, customer_id)
        await repo.save_links((link,))

        result = await repo.get_link(KnownIdentifier(KnownIdentifierType.PHONE, value))

        assert result is None


class TestFindLinks:
    async def test_returns_empty_for_empty_input(
        self, repo: IdentityRepository
    ) -> None:
        result = await repo.find_links(())

        assert result == ()

    async def test_finds_single_link(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        email = _email()
        await _insert_customer_profile(session, customer_id)
        await repo.save_links(
            (_make_link(KnownIdentifierType.EMAIL, email, customer_id),)
        )

        result = await repo.find_links(
            (KnownIdentifier(KnownIdentifierType.EMAIL, email),)
        )

        assert len(result) == 1
        assert result[0].customer_id == customer_id

    async def test_finds_multiple_links_in_one_query(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        email1 = _email('a')
        email2 = _email('b')
        await _insert_customer_profile(session, customer_id)
        await repo.save_links(
            (
                _make_link(KnownIdentifierType.EMAIL, email1, customer_id),
                _make_link(KnownIdentifierType.EMAIL, email2, customer_id),
            )
        )

        result = await repo.find_links(
            (
                KnownIdentifier(KnownIdentifierType.EMAIL, email1),
                KnownIdentifier(KnownIdentifierType.EMAIL, email2),
            )
        )

        values = {link.identity_value for link in result}
        assert email1 in values
        assert email2 in values

    async def test_finds_links_across_types(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        email = _email()
        phone = f'+7000{uuid.uuid4().hex[:7]}'
        await _insert_customer_profile(session, customer_id)
        await repo.save_links(
            (
                _make_link(KnownIdentifierType.EMAIL, email, customer_id),
                _make_link(KnownIdentifierType.PHONE, phone, customer_id),
            )
        )

        result = await repo.find_links(
            (
                KnownIdentifier(KnownIdentifierType.EMAIL, email),
                KnownIdentifier(KnownIdentifierType.PHONE, phone),
            )
        )

        assert len(result) == 2
        types = {link.identity_type for link in result}
        assert KnownIdentifierType.EMAIL in types
        assert KnownIdentifierType.PHONE in types

    async def test_skips_identifiers_with_no_link(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        email = _email()
        await _insert_customer_profile(session, customer_id)
        await repo.save_links(
            (_make_link(KnownIdentifierType.EMAIL, email, customer_id),)
        )

        result = await repo.find_links(
            (
                KnownIdentifier(KnownIdentifierType.EMAIL, email),
                KnownIdentifier(KnownIdentifierType.EMAIL, 'ghost@example.com'),
            )
        )

        assert len(result) == 1
        assert result[0].identity_value == email

    async def test_links_from_different_customers_are_all_returned(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        cust_a = _uid()
        cust_b = _uid()
        email_a = _email('a')
        email_b = _email('b')
        await _insert_customer_profile(session, cust_a)
        await _insert_customer_profile(session, cust_b)
        await repo.save_links((_make_link(KnownIdentifierType.EMAIL, email_a, cust_a),))
        await repo.save_links((_make_link(KnownIdentifierType.EMAIL, email_b, cust_b),))

        result = await repo.find_links(
            (
                KnownIdentifier(KnownIdentifierType.EMAIL, email_a),
                KnownIdentifier(KnownIdentifierType.EMAIL, email_b),
            )
        )

        customer_ids = {link.customer_id for link in result}
        assert cust_a in customer_ids
        assert cust_b in customer_ids


class TestSaveLinks:
    async def test_save_single_link(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        email = _email()
        await _insert_customer_profile(session, customer_id)
        link = _make_link(KnownIdentifierType.EMAIL, email, customer_id)

        await repo.save_links((link,))

        found = await repo.get_link(KnownIdentifier(KnownIdentifierType.EMAIL, email))
        assert found is not None
        assert found.customer_id == customer_id

    async def test_save_empty_tuple_is_noop(self, repo: IdentityRepository) -> None:
        await repo.save_links(())

    async def test_multiple_identifiers_same_customer(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        email1 = _email('first')
        email2 = _email('second')
        await _insert_customer_profile(session, customer_id)

        await repo.save_links(
            (
                _make_link(KnownIdentifierType.EMAIL, email1, customer_id),
                _make_link(KnownIdentifierType.EMAIL, email2, customer_id),
            )
        )

        result = await repo.find_links(
            (
                KnownIdentifier(KnownIdentifierType.EMAIL, email1),
                KnownIdentifier(KnownIdentifierType.EMAIL, email2),
            )
        )
        customer_ids = {link.customer_id for link in result}
        assert customer_ids == {customer_id}

    async def test_idempotent_save_same_link_twice(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        customer_id = _uid()
        email = _email()
        await _insert_customer_profile(session, customer_id)
        link = _make_link(KnownIdentifierType.EMAIL, email, customer_id)

        await repo.save_links((link,))
        await repo.save_links((link,))

        found = await repo.get_link(KnownIdentifier(KnownIdentifierType.EMAIL, email))
        assert found is not None
        assert found.customer_id == customer_id

    async def test_conflict_raises_integrity_error(
        self, repo: IdentityRepository, session: AsyncSession
    ) -> None:
        cust_a = _uid()
        cust_b = _uid()
        email = _email()
        await _insert_customer_profile(session, cust_a)
        await _insert_customer_profile(session, cust_b)
        await repo.save_links((_make_link(KnownIdentifierType.EMAIL, email, cust_a),))

        with pytest.raises(sa.exc.IntegrityError):
            await session.execute(
                sa.text(
                    """
                    INSERT INTO identity_links (identity_type, identity_value, customer_id, created_at)
                    VALUES (:identity_type, :identity_value, :customer_id, NOW())
                    """
                ),
                {
                    'identity_type': KnownIdentifierType.EMAIL.value,
                    'identity_value': email,
                    'customer_id': cust_b,
                },
            )
