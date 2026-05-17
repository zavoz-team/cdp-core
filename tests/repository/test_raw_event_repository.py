import datetime
import uuid

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from adapter.config.loader import load_config
from domain.event import RawEvent, RawEventIdentifiers, RawEventProcessingStatus
from domain.identity import KnownIdentifier, KnownIdentifierType
from repository.raw_event import RawEventRepository
from usecase.dto import RawEventRecordOutcome


def _uid() -> str:
    return f'evt-{uuid.uuid4().hex[:12]}'


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _make_event(
    event_id: str | None = None,
    event_type: str = 'page_view',
    source: str = 'web',
    known_email: str | None = 'user@example.com',
    anonymous_id: str | None = None,
    attributes: dict | None = None,
    payload: dict | None = None,
    trace_context: dict | None = None,
) -> RawEvent:
    now = _utcnow()
    if known_email is not None:
        identifiers = RawEventIdentifiers(
            known=(KnownIdentifier(KnownIdentifierType.EMAIL, known_email),),
            anonymous_id=anonymous_id,
        )
    else:
        identifiers = RawEventIdentifiers(anonymous_id=anonymous_id or 'anon-default')
    return RawEvent(
        event_id=event_id or _uid(),
        event_type=event_type,
        source=source,
        occurred_at=now,
        received_at=now,
        created_at=now,
        identifiers=identifiers,
        attributes=attributes or {},
        payload=payload or {},
        trace_context=trace_context or {},
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
def repo(session: AsyncSession) -> RawEventRepository:
    return RawEventRepository(session)


class TestRecordReceived:
    async def test_new_event_returns_created(self, repo: RawEventRepository) -> None:
        event = _make_event()

        result = await repo.record_received(event)

        assert result.outcome == RawEventRecordOutcome.CREATED
        assert result.event_id == event.event_id

    async def test_duplicate_returns_duplicate(self, repo: RawEventRepository) -> None:
        event = _make_event()
        await repo.record_received(event)

        result = await repo.record_received(event)

        assert result.outcome == RawEventRecordOutcome.DUPLICATE

    async def test_sets_received_status(self, repo: RawEventRepository) -> None:
        event = _make_event()
        await repo.record_received(event)

        saved = await repo.get_by_event_id(event.event_id)

        assert saved is not None
        assert saved.processing_status == RawEventProcessingStatus.RECEIVED

    async def test_two_different_events_both_created(self, repo: RawEventRepository) -> None:
        a = _make_event()
        b = _make_event()

        r1 = await repo.record_received(a)
        r2 = await repo.record_received(b)

        assert r1.outcome == RawEventRecordOutcome.CREATED
        assert r2.outcome == RawEventRecordOutcome.CREATED


class TestGetByEventId:
    async def test_returns_none_for_missing(self, repo: RawEventRepository) -> None:
        result = await repo.get_by_event_id('does-not-exist-xyz')

        assert result is None

    async def test_returns_correct_scalar_fields(self, repo: RawEventRepository) -> None:
        event = _make_event(source='mobile', event_type='add_to_cart')
        await repo.record_received(event)

        found = await repo.get_by_event_id(event.event_id)

        assert found is not None
        assert found.event_id == event.event_id
        assert found.source == 'mobile'
        assert str(found.event_type) == 'add_to_cart'
        assert found.error_reason is None

    async def test_round_trips_known_identifier(self, repo: RawEventRepository) -> None:
        event = _make_event(known_email='rt@example.com')
        await repo.record_received(event)

        found = await repo.get_by_event_id(event.event_id)

        assert found is not None
        assert len(found.identifiers.known) == 1
        assert found.identifiers.known[0].identifier_type == KnownIdentifierType.EMAIL
        assert found.identifiers.known[0].value == 'rt@example.com'
        assert found.identifiers.anonymous_id is None

    async def test_round_trips_anonymous_only_identifiers(self, repo: RawEventRepository) -> None:
        event = _make_event(known_email=None, anonymous_id='anon-abc123')
        await repo.record_received(event)

        found = await repo.get_by_event_id(event.event_id)

        assert found is not None
        assert found.identifiers.is_anonymous_only
        assert found.identifiers.anonymous_id == 'anon-abc123'
        assert len(found.identifiers.known) == 0

    async def test_round_trips_known_and_anonymous_identifiers(
        self, repo: RawEventRepository
    ) -> None:
        event = _make_event(known_email='both@example.com', anonymous_id='anon-xyz')
        await repo.record_received(event)

        found = await repo.get_by_event_id(event.event_id)

        assert found is not None
        assert found.identifiers.known[0].value == 'both@example.com'
        assert found.identifiers.anonymous_id == 'anon-xyz'

    async def test_round_trips_attributes(self, repo: RawEventRepository) -> None:
        event = _make_event(attributes={'page': '/checkout', 'ref': 'banner'})
        await repo.record_received(event)

        found = await repo.get_by_event_id(event.event_id)

        assert found is not None
        assert found.attributes['page'] == '/checkout'
        assert found.attributes['ref'] == 'banner'

    async def test_round_trips_payload(self, repo: RawEventRepository) -> None:
        event = _make_event(payload={'order_id': 'ord-99', 'amount': 199.50})
        await repo.record_received(event)

        found = await repo.get_by_event_id(event.event_id)

        assert found is not None
        assert found.payload['order_id'] == 'ord-99'

    async def test_round_trips_trace_context(self, repo: RawEventRepository) -> None:
        event = _make_event(trace_context={'trace_id': 'abc', 'span_id': 'def'})
        await repo.record_received(event)

        found = await repo.get_by_event_id(event.event_id)

        assert found is not None
        assert found.trace_context['trace_id'] == 'abc'


class TestMarkProcessed:
    async def test_sets_processed_status(self, repo: RawEventRepository) -> None:
        event = _make_event()
        await repo.record_received(event)

        await repo.mark_processed(event.event_id)

        found = await repo.get_by_event_id(event.event_id)
        assert found is not None
        assert found.processing_status == RawEventProcessingStatus.PROCESSED
        assert found.error_reason is None


class TestMarkIgnoredAnonymous:
    async def test_sets_ignored_anonymous_status(self, repo: RawEventRepository) -> None:
        event = _make_event()
        await repo.record_received(event)

        await repo.mark_ignored_anonymous(event.event_id)

        found = await repo.get_by_event_id(event.event_id)
        assert found is not None
        assert found.processing_status == RawEventProcessingStatus.IGNORED_ANONYMOUS
        assert found.error_reason is None


class TestMarkSentToDlq:
    async def test_sets_sent_to_dlq_status_and_reason(self, repo: RawEventRepository) -> None:
        event = _make_event()
        await repo.record_received(event)

        await repo.mark_sent_to_dlq(event.event_id, 'unsupported_event_type')

        found = await repo.get_by_event_id(event.event_id)
        assert found is not None
        assert found.processing_status == RawEventProcessingStatus.SENT_TO_DLQ
        assert found.error_reason == 'unsupported_event_type'


class TestMarkFailed:
    async def test_sets_failed_status_and_reason(self, repo: RawEventRepository) -> None:
        event = _make_event()
        await repo.record_received(event)

        await repo.mark_failed(event.event_id, 'processing_error')

        found = await repo.get_by_event_id(event.event_id)
        assert found is not None
        assert found.processing_status == RawEventProcessingStatus.FAILED
        assert found.error_reason == 'processing_error'

    async def test_status_can_be_overwritten(self, repo: RawEventRepository) -> None:
        event = _make_event()
        await repo.record_received(event)
        await repo.mark_sent_to_dlq(event.event_id, 'first_reason')

        await repo.mark_failed(event.event_id, 'second_reason')

        found = await repo.get_by_event_id(event.event_id)
        assert found is not None
        assert found.processing_status == RawEventProcessingStatus.FAILED
        assert found.error_reason == 'second_reason'
