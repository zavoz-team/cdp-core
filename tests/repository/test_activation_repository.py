import datetime
import uuid
from dataclasses import replace

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from adapter.config.loader import load_config
from domain.export_job import (
    ActivationDelivery,
    ActivationDeliveryStatus,
    ActivationDestination,
    ActivationJob,
    ActivationJobStatus,
    DestinationType,
)
from domain.segment import SegmentId
from repository.activation import ActivationDeliveryRepository, ActivationJobRepository


def _uid(prefix: str = 'job') -> str:
    return f'{prefix}-{uuid.uuid4().hex[:10]}'


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _make_destination(url: str = 'https://hooks.example.com/cdp') -> ActivationDestination:
    return ActivationDestination(
        destination_type=DestinationType.WEBHOOK,
        url=url,
    )


def _make_job(
    job_id: str | None = None,
    segment_id: SegmentId = SegmentId.ACTIVE,
    status: ActivationJobStatus = ActivationJobStatus.PENDING,
    members_count: int = 0,
    requested_by: str | None = None,
    completed_at: datetime.datetime | None = None,
    error_reason: str | None = None,
) -> ActivationJob:
    return ActivationJob(
        job_id=job_id or _uid(),
        segment_id=segment_id,
        destination=_make_destination(),
        requested_at=_utcnow(),
        status=status,
        members_count=members_count,
        requested_by=requested_by,
        completed_at=completed_at,
        error_reason=error_reason,
    )


def _make_delivery(
    job_id: str,
    segment_id: SegmentId = SegmentId.ACTIVE,
    members_count: int = 5,
    status: ActivationDeliveryStatus = ActivationDeliveryStatus.SUCCEEDED,
    response_status_code: int | None = 200,
    error_reason: str | None = None,
) -> ActivationDelivery:
    now = _utcnow()
    return ActivationDelivery(
        delivery_id=_uid('dlv'),
        job_id=job_id,
        segment_id=segment_id,
        destination=_make_destination(),
        attempt_number=1,
        members_count=members_count,
        status=status,
        requested_at=now,
        completed_at=now,
        response_status_code=response_status_code,
        error_reason=error_reason,
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
def job_repo(session: AsyncSession) -> ActivationJobRepository:
    return ActivationJobRepository(session)


@pytest.fixture
def delivery_repo(session: AsyncSession) -> ActivationDeliveryRepository:
    return ActivationDeliveryRepository(session)


class TestGetByJobId:
    async def test_returns_none_for_missing(self, job_repo: ActivationJobRepository) -> None:
        result = await job_repo.get_by_job_id('does-not-exist')

        assert result is None

    async def test_returns_job_after_save(self, job_repo: ActivationJobRepository) -> None:
        job = _make_job()
        await job_repo.save_job(job)

        found = await job_repo.get_by_job_id(job.job_id)

        assert found is not None
        assert found.job_id == job.job_id

    async def test_round_trips_all_fields(self, job_repo: ActivationJobRepository) -> None:
        job = _make_job(
            segment_id=SegmentId.VIP,
            status=ActivationJobStatus.PENDING,
            requested_by='admin@example.com',
        )
        await job_repo.save_job(job)

        found = await job_repo.get_by_job_id(job.job_id)

        assert found is not None
        assert found.segment_id == SegmentId.VIP
        assert found.status == ActivationJobStatus.PENDING
        assert found.destination.destination_type == DestinationType.WEBHOOK
        assert found.destination.url == 'https://hooks.example.com/cdp'
        assert found.requested_by == 'admin@example.com'
        assert found.completed_at is None
        assert found.error_reason is None


class TestSaveJobLifecycle:
    async def test_pending_to_running(self, job_repo: ActivationJobRepository) -> None:
        job = _make_job(status=ActivationJobStatus.PENDING)
        await job_repo.save_job(job)

        running = replace(job, status=ActivationJobStatus.RUNNING)
        await job_repo.save_job(running)

        found = await job_repo.get_by_job_id(job.job_id)
        assert found is not None
        assert found.status == ActivationJobStatus.RUNNING

    async def test_running_to_succeeded(self, job_repo: ActivationJobRepository) -> None:
        job = _make_job(status=ActivationJobStatus.PENDING)
        await job_repo.save_job(job)
        running = replace(job, status=ActivationJobStatus.RUNNING)
        await job_repo.save_job(running)

        now = _utcnow()
        succeeded = replace(
            running,
            status=ActivationJobStatus.SUCCEEDED,
            members_count=42,
            completed_at=now,
        )
        await job_repo.save_job(succeeded)

        found = await job_repo.get_by_job_id(job.job_id)
        assert found is not None
        assert found.status == ActivationJobStatus.SUCCEEDED
        assert found.members_count == 42
        assert found.completed_at is not None

    async def test_running_to_failed_with_reason(self, job_repo: ActivationJobRepository) -> None:
        job = _make_job(status=ActivationJobStatus.PENDING)
        await job_repo.save_job(job)
        running = replace(job, status=ActivationJobStatus.RUNNING)
        await job_repo.save_job(running)

        failed = replace(
            running,
            status=ActivationJobStatus.FAILED,
            error_reason='delivery dependency failed',
            completed_at=_utcnow(),
        )
        await job_repo.save_job(failed)

        found = await job_repo.get_by_job_id(job.job_id)
        assert found is not None
        assert found.status == ActivationJobStatus.FAILED
        assert found.error_reason == 'delivery dependency failed'

    async def test_upsert_does_not_create_duplicate(
        self, job_repo: ActivationJobRepository
    ) -> None:
        job = _make_job()
        await job_repo.save_job(job)
        updated = replace(job, status=ActivationJobStatus.RUNNING)
        await job_repo.save_job(updated)

        await job_repo.count_jobs()
        all_jobs = await job_repo.list_jobs(limit=1000)
        matching = [j for j in all_jobs if j.job_id == job.job_id]
        assert len(matching) == 1


class TestListJobs:
    async def test_returns_saved_jobs(self, job_repo: ActivationJobRepository) -> None:
        j1 = _make_job()
        j2 = _make_job()
        await job_repo.save_job(j1)
        await job_repo.save_job(j2)

        result = await job_repo.list_jobs(limit=1000)

        ids = {j.job_id for j in result}
        assert j1.job_id in ids
        assert j2.job_id in ids

    async def test_filters_by_status(self, job_repo: ActivationJobRepository) -> None:
        pending = _make_job(status=ActivationJobStatus.PENDING)
        succeeded = _make_job(status=ActivationJobStatus.SUCCEEDED)
        await job_repo.save_job(pending)
        await job_repo.save_job(succeeded)

        result = await job_repo.list_jobs(limit=1000, status=ActivationJobStatus.PENDING)

        ids = {j.job_id for j in result}
        assert pending.job_id in ids
        assert succeeded.job_id not in ids

    async def test_filters_by_segment_id(self, job_repo: ActivationJobRepository) -> None:
        vip_job = _make_job(segment_id=SegmentId.VIP)
        new_user_job = _make_job(segment_id=SegmentId.NEW_USER)
        await job_repo.save_job(vip_job)
        await job_repo.save_job(new_user_job)

        result = await job_repo.list_jobs(limit=1000, segment_id=SegmentId.VIP)

        ids = {j.job_id for j in result}
        assert vip_job.job_id in ids
        assert new_user_job.job_id not in ids

    async def test_filters_by_status_and_segment_id_combined(
        self, job_repo: ActivationJobRepository
    ) -> None:
        match = _make_job(segment_id=SegmentId.ACTIVE, status=ActivationJobStatus.FAILED)
        no_match_wrong_status = _make_job(
            segment_id=SegmentId.ACTIVE, status=ActivationJobStatus.SUCCEEDED
        )
        no_match_wrong_segment = _make_job(
            segment_id=SegmentId.VIP, status=ActivationJobStatus.FAILED
        )
        await job_repo.save_job(match)
        await job_repo.save_job(no_match_wrong_status)
        await job_repo.save_job(no_match_wrong_segment)

        result = await job_repo.list_jobs(
            limit=1000,
            segment_id=SegmentId.ACTIVE,
            status=ActivationJobStatus.FAILED,
        )

        ids = {j.job_id for j in result}
        assert match.job_id in ids
        assert no_match_wrong_status.job_id not in ids
        assert no_match_wrong_segment.job_id not in ids

    async def test_respects_limit_and_offset(self, job_repo: ActivationJobRepository) -> None:
        for _ in range(3):
            await job_repo.save_job(_make_job())

        page1 = await job_repo.list_jobs(limit=2, offset=0)
        page2 = await job_repo.list_jobs(limit=2, offset=2)

        assert len(page1) == 2
        assert {j.job_id for j in page1}.isdisjoint({j.job_id for j in page2})


class TestCountJobs:
    async def test_counts_all_jobs(self, job_repo: ActivationJobRepository) -> None:
        before = await job_repo.count_jobs()
        await job_repo.save_job(_make_job())
        await job_repo.save_job(_make_job())

        after = await job_repo.count_jobs()

        assert after == before + 2

    async def test_counts_with_status_filter(self, job_repo: ActivationJobRepository) -> None:
        await job_repo.save_job(_make_job(status=ActivationJobStatus.SUCCEEDED))
        before_failed = await job_repo.count_jobs(status=ActivationJobStatus.FAILED)

        await job_repo.save_job(_make_job(status=ActivationJobStatus.FAILED))

        after_failed = await job_repo.count_jobs(status=ActivationJobStatus.FAILED)
        assert after_failed == before_failed + 1

    async def test_counts_with_segment_filter(self, job_repo: ActivationJobRepository) -> None:
        before = await job_repo.count_jobs(segment_id=SegmentId.NEW_USER)
        await job_repo.save_job(_make_job(segment_id=SegmentId.NEW_USER))

        after = await job_repo.count_jobs(segment_id=SegmentId.NEW_USER)

        assert after == before + 1


class TestGetForJob:
    async def test_returns_none_for_missing(
        self, delivery_repo: ActivationDeliveryRepository
    ) -> None:
        result = await delivery_repo.get_for_job('does-not-exist')

        assert result is None

    async def test_returns_delivery_after_save(
        self,
        job_repo: ActivationJobRepository,
        delivery_repo: ActivationDeliveryRepository,
    ) -> None:
        job = _make_job()
        await job_repo.save_job(job)
        delivery = _make_delivery(job_id=job.job_id, segment_id=job.segment_id)
        await delivery_repo.save_delivery(delivery)

        found = await delivery_repo.get_for_job(job.job_id)

        assert found is not None
        assert found.delivery_id == delivery.delivery_id
        assert found.job_id == job.job_id

    async def test_round_trips_delivery_fields(
        self,
        job_repo: ActivationJobRepository,
        delivery_repo: ActivationDeliveryRepository,
    ) -> None:
        job = _make_job(segment_id=SegmentId.VIP, members_count=99)
        await job_repo.save_job(job)
        delivery = _make_delivery(
            job_id=job.job_id,
            segment_id=SegmentId.VIP,
            members_count=99,
            status=ActivationDeliveryStatus.SUCCEEDED,
            response_status_code=200,
        )
        await delivery_repo.save_delivery(delivery)

        found = await delivery_repo.get_for_job(job.job_id)

        assert found is not None
        assert found.members_count == 99
        assert found.status == ActivationDeliveryStatus.SUCCEEDED
        assert found.response_status_code == 200
        assert found.segment_id == SegmentId.VIP
        assert found.destination.url == delivery.destination.url

    async def test_round_trips_failed_delivery(
        self,
        job_repo: ActivationJobRepository,
        delivery_repo: ActivationDeliveryRepository,
    ) -> None:
        job = _make_job()
        await job_repo.save_job(job)
        delivery = _make_delivery(
            job_id=job.job_id,
            segment_id=job.segment_id,
            status=ActivationDeliveryStatus.FAILED,
            response_status_code=503,
            error_reason='upstream timeout',
        )
        await delivery_repo.save_delivery(delivery)

        found = await delivery_repo.get_for_job(job.job_id)

        assert found is not None
        assert found.status == ActivationDeliveryStatus.FAILED
        assert found.error_reason == 'upstream timeout'
        assert found.response_status_code == 503

    async def test_upsert_updates_delivery(
        self,
        job_repo: ActivationJobRepository,
        delivery_repo: ActivationDeliveryRepository,
    ) -> None:
        job = _make_job()
        await job_repo.save_job(job)
        delivery = _make_delivery(
            job_id=job.job_id,
            segment_id=job.segment_id,
            status=ActivationDeliveryStatus.FAILED,
            response_status_code=500,
            error_reason='initial error',
        )
        await delivery_repo.save_delivery(delivery)

        updated = replace(
            delivery,
            status=ActivationDeliveryStatus.SUCCEEDED,
            response_status_code=200,
            error_reason=None,
        )
        await delivery_repo.save_delivery(updated)

        found = await delivery_repo.get_for_job(job.job_id)
        assert found is not None
        assert found.status == ActivationDeliveryStatus.SUCCEEDED
        assert found.error_reason is None
