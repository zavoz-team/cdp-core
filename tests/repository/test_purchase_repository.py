import datetime
import uuid
from decimal import Decimal

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from adapter.config.loader import load_config
from adapter.observability.noop import NoopLogger, NoopTracer
from domain.profile import Currency
from repository.purchase import PurchaseRepository
from usecase.dto import ProcessedPurchase, PurchaseRecordOutcome


def _uid(prefix: str = 'ord') -> str:
    return f'{prefix}-{uuid.uuid4().hex[:10]}'


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _make_purchase(
    source: str | None = None,
    order_id: str | None = None,
    customer_id: str | None = None,
    event_id: str | None = None,
    amount: str = '1500.00',
    currency: Currency = Currency.RUB,
    occurred_at: datetime.datetime | None = None,
) -> ProcessedPurchase:
    return ProcessedPurchase(
        source=source or 'web',
        order_id=order_id or _uid(),
        customer_id=customer_id or _uid('cust'),
        event_id=event_id or _uid('evt'),
        amount=Decimal(amount),
        currency=currency,
        occurred_at=occurred_at or _utcnow(),
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
def repo(session: AsyncSession) -> PurchaseRepository:
    return PurchaseRepository(session, NoopLogger(), NoopTracer())


class TestGetBySourceOrderId:
    async def test_returns_none_for_missing(self, repo: PurchaseRepository) -> None:
        result = await repo.get_by_source_order_id('web', 'does-not-exist')

        assert result is None

    async def test_returns_purchase_after_record(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        purchase = _make_purchase()
        await _insert_customer_profile(session, purchase.customer_id)
        await repo.record_processed_purchase(purchase)

        found = await repo.get_by_source_order_id(purchase.source, purchase.order_id)

        assert found is not None
        assert found.source == purchase.source
        assert found.order_id == purchase.order_id

    async def test_round_trips_all_fields(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        now = _utcnow()
        purchase = _make_purchase(
            source='mobile',
            order_id=_uid(),
            customer_id=_uid('cust'),
            event_id=_uid('evt'),
            amount='9999.99',
            occurred_at=now,
        )
        await _insert_customer_profile(session, purchase.customer_id)
        await repo.record_processed_purchase(purchase)

        found = await repo.get_by_source_order_id(purchase.source, purchase.order_id)

        assert found is not None
        assert found.customer_id == purchase.customer_id
        assert found.event_id == purchase.event_id
        assert found.amount == Decimal('9999.99')
        assert found.currency == Currency.RUB

    async def test_different_sources_same_order_id_are_independent(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        order_id = _uid()
        p1 = _make_purchase(source='web', order_id=order_id)
        p2 = _make_purchase(source='mobile', order_id=order_id)
        await _insert_customer_profile(session, p1.customer_id)
        await _insert_customer_profile(session, p2.customer_id)
        await repo.record_processed_purchase(p1)
        await repo.record_processed_purchase(p2)

        found_web = await repo.get_by_source_order_id('web', order_id)
        found_mobile = await repo.get_by_source_order_id('mobile', order_id)

        assert found_web is not None
        assert found_mobile is not None
        assert found_web.customer_id == p1.customer_id
        assert found_mobile.customer_id == p2.customer_id


class TestRecordProcessedPurchase:
    async def test_new_purchase_returns_recorded(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        purchase = _make_purchase()
        await _insert_customer_profile(session, purchase.customer_id)

        result = await repo.record_processed_purchase(purchase)

        assert result.outcome == PurchaseRecordOutcome.RECORDED
        assert result.source == purchase.source
        assert result.order_id == purchase.order_id
        assert result.existing_purchase is None

    async def test_exact_duplicate_returns_duplicate(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        purchase = _make_purchase()
        await _insert_customer_profile(session, purchase.customer_id)
        await repo.record_processed_purchase(purchase)

        result = await repo.record_processed_purchase(purchase)

        assert result.outcome == PurchaseRecordOutcome.DUPLICATE
        assert result.existing_purchase is not None
        assert result.existing_purchase.order_id == purchase.order_id

    async def test_duplicate_does_not_create_second_row(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        purchase = _make_purchase()
        await _insert_customer_profile(session, purchase.customer_id)
        await repo.record_processed_purchase(purchase)
        await repo.record_processed_purchase(purchase)

        found = await repo.get_by_source_order_id(purchase.source, purchase.order_id)
        assert found is not None
        assert found.amount == purchase.amount

    async def test_same_key_different_amount_returns_conflict(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        source = 'web'
        order_id = _uid()
        customer_id = _uid('cust')
        original = _make_purchase(
            source=source, order_id=order_id, customer_id=customer_id, amount='500.00'
        )
        conflicting = _make_purchase(
            source=source, order_id=order_id, customer_id=customer_id, amount='999.00'
        )
        await _insert_customer_profile(session, customer_id)
        await repo.record_processed_purchase(original)

        result = await repo.record_processed_purchase(conflicting)

        assert result.outcome == PurchaseRecordOutcome.CONFLICT
        assert result.existing_purchase is not None
        assert result.existing_purchase.amount == Decimal('500.00')

    async def test_same_key_different_customer_id_returns_conflict(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        source = 'web'
        order_id = _uid()
        original = _make_purchase(
            source=source, order_id=order_id, customer_id=_uid('cust'), amount='300.00'
        )
        conflicting = _make_purchase(
            source=source, order_id=order_id, customer_id=_uid('cust'), amount='300.00'
        )
        await _insert_customer_profile(session, original.customer_id)
        await repo.record_processed_purchase(original)

        result = await repo.record_processed_purchase(conflicting)

        assert result.outcome == PurchaseRecordOutcome.CONFLICT

    async def test_conflict_preserves_original_purchase(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        source = 'web'
        order_id = _uid()
        original_customer = _uid('cust')
        original = _make_purchase(
            source=source,
            order_id=order_id,
            customer_id=original_customer,
            amount='100.00',
        )
        conflicting = _make_purchase(
            source=source, order_id=order_id, customer_id=_uid('cust'), amount='200.00'
        )
        await _insert_customer_profile(session, original_customer)
        await repo.record_processed_purchase(original)
        await repo.record_processed_purchase(conflicting)

        found = await repo.get_by_source_order_id(source, order_id)

        assert found is not None
        assert found.customer_id == original_customer
        assert found.amount == Decimal('100.00')

    async def test_zero_amount_is_valid(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        purchase = _make_purchase(amount='0.00')
        await _insert_customer_profile(session, purchase.customer_id)

        result = await repo.record_processed_purchase(purchase)

        assert result.outcome == PurchaseRecordOutcome.RECORDED

    async def test_large_amount_round_trips(
        self, repo: PurchaseRepository, session: AsyncSession
    ) -> None:
        purchase = _make_purchase(amount='9999999.99')
        await _insert_customer_profile(session, purchase.customer_id)
        await repo.record_processed_purchase(purchase)

        found = await repo.get_by_source_order_id(purchase.source, purchase.order_id)

        assert found is not None
        assert found.amount == Decimal('9999999.99')
