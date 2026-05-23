import asyncio
from dataclasses import dataclass, field
from datetime import datetime

import pytest

from domain.event import EventType
from domain.export_job import (
    ActivationDelivery,
    ActivationDeliveryStatus,
    ActivationDestination,
    ActivationJob,
    ActivationJobStatus,
    DestinationType,
)
from domain.identity import CustomerIdentifiers
from domain.profile import CustomerProfile, RecentEvent
from domain.segment import SegmentDefinition, SegmentId, SegmentMembership
from usecase.dto import OutboundWebhookDeliveryResult, SegmentExportPayload
from usecase.error import (
    ExportJobNotFoundUseCaseError,
    SegmentDisabledUseCaseError,
    SegmentNotFoundUseCaseError,
    UseCaseDependencyError,
    UseCaseValidationError,
)
from usecase.export import ActivationService
from usecase.interface import (
    ActivationDeliveryRepository,
    ActivationJobRepository,
    ExportTransaction,
)

NOW = datetime(2026, 1, 1, 12, 0, 0)
DEFAULT_SEGMENT_DEFINITION = SegmentDefinition(
    SegmentId.NEW_USER,
    'New User',
    'New User',
)


@dataclass
class FakeExportState:
    jobs: dict[str, ActivationJob] = field(default_factory=dict)
    job_saves: list[ActivationJob] = field(default_factory=list)
    deliveries: dict[str, ActivationDelivery] = field(default_factory=dict)
    delivery_saves: list[ActivationDelivery] = field(default_factory=list)
    commits: int = 0
    rollbacks: int = 0
    save_job_calls: int = 0
    save_job_errors_remaining: int = 0
    save_job_error_on_call: int | None = None
    save_delivery_errors_remaining: int = 0
    commit_errors_remaining: int = 0
    rollback_errors_remaining: int = 0


class FakeActivationJobRepository:
    def __init__(self, state: FakeExportState) -> None:
        self._state = state

    async def get_by_job_id(self, job_id: str) -> ActivationJob | None:
        return self._state.jobs.get(job_id)

    async def list_jobs(
        self,
        limit: int = 50,
        offset: int = 0,
        segment_id: SegmentId | None = None,
        status: ActivationJobStatus | None = None,
    ) -> tuple[ActivationJob, ...]:
        jobs = tuple(
            job
            for job in self._state.jobs.values()
            if (segment_id is None or job.segment_id == segment_id)
            and (status is None or job.status == status)
        )
        return jobs[offset : offset + limit]

    async def count_jobs(
        self,
        limit: int = 50,
        offset: int = 0,
        segment_id: SegmentId | None = None,
        status: ActivationJobStatus | None = None,
    ) -> int:
        return len(
            tuple(
                job
                for job in self._state.jobs.values()
                if (segment_id is None or job.segment_id == segment_id)
                and (status is None or job.status == status)
            )
        )

    async def save_job(self, job: ActivationJob) -> None:
        self._state.save_job_calls += 1
        if self._state.save_job_calls == self._state.save_job_error_on_call:
            raise UseCaseDependencyError('job repository unavailable')

        if self._state.save_job_errors_remaining > 0:
            self._state.save_job_errors_remaining -= 1
            raise UseCaseDependencyError('job repository unavailable')

        self._state.jobs[job.job_id] = job
        self._state.job_saves.append(job)


class FakeActivationDeliveryRepository:
    def __init__(self, state: FakeExportState) -> None:
        self._state = state

    async def get_for_job(self, job_id: str) -> ActivationDelivery | None:
        return self._state.deliveries.get(job_id)

    async def save_delivery(self, delivery: ActivationDelivery) -> None:
        if self._state.save_delivery_errors_remaining > 0:
            self._state.save_delivery_errors_remaining -= 1
            raise UseCaseDependencyError('delivery repository unavailable')

        self._state.deliveries[delivery.job_id] = delivery
        self._state.delivery_saves.append(delivery)


class FakeExportTransaction:
    jobs: ActivationJobRepository
    deliveries: ActivationDeliveryRepository

    def __init__(self, state: FakeExportState) -> None:
        self._state = state
        self.jobs = FakeActivationJobRepository(state)
        self.deliveries = FakeActivationDeliveryRepository(state)

    async def commit(self) -> None:
        if self._state.commit_errors_remaining > 0:
            self._state.commit_errors_remaining -= 1
            raise UseCaseDependencyError('commit unavailable')

        self._state.commits += 1

    async def rollback(self) -> None:
        if self._state.rollback_errors_remaining > 0:
            self._state.rollback_errors_remaining -= 1
            raise UseCaseDependencyError('rollback unavailable')

        self._state.rollbacks += 1


class FakeExportUnitOfWork:
    def __init__(self, state: FakeExportState) -> None:
        self._state = state

    async def begin(self) -> ExportTransaction:
        return FakeExportTransaction(self._state)


class FakeSegmentRepository:
    def __init__(
        self,
        members: tuple[CustomerProfile, ...],
        definition: SegmentDefinition | None = DEFAULT_SEGMENT_DEFINITION,
    ) -> None:
        self._members = members
        self._definition = definition

    async def get_definition(self, segment_id: SegmentId) -> SegmentDefinition | None:
        if self._definition is None or self._definition.segment_id != segment_id:
            return None

        return self._definition

    async def list_definitions(
        self,
        include_disabled: bool = False,
    ) -> tuple[SegmentDefinition, ...]:
        if self._definition is None:
            return ()

        if self._definition.is_active or include_disabled:
            return (self._definition,)

        return ()

    async def count_members(self, segment_id: SegmentId) -> int:
        return len(self._members)

    async def list_memberships(
        self,
        segment_id: SegmentId,
        limit: int,
        offset: int,
    ) -> tuple[SegmentMembership, ...]:
        return tuple(
            SegmentMembership(segment_id, member.customer_id, NOW, NOW)
            for member in self._members[offset : offset + limit]
        )

    async def list_member_profiles(
        self,
        segment_id: SegmentId,
        limit: int,
        offset: int,
    ) -> tuple[CustomerProfile, ...]:
        return self._members[offset : offset + limit]

    async def replace_profile_memberships(
        self,
        customer_id: str,
        memberships: tuple[SegmentMembership, ...],
    ) -> None:
        raise AssertionError('export tests must not replace memberships')


class FailingMemberSegmentRepository(FakeSegmentRepository):
    async def count_members(self, segment_id: SegmentId) -> int:
        raise RuntimeError('members unavailable')


class BlankFailingMemberSegmentRepository(FakeSegmentRepository):
    async def count_members(self, segment_id: SegmentId) -> int:
        raise RuntimeError()


class FakeWebhookGateway:
    def __init__(
        self,
        result: OutboundWebhookDeliveryResult | None = None,
        error: UseCaseDependencyError | None = None,
    ) -> None:
        self._result = result
        self._error = error
        self.payloads: list[SegmentExportPayload] = []

    async def send_segment_export(
        self,
        destination: ActivationDestination,
        payload: SegmentExportPayload,
    ) -> OutboundWebhookDeliveryResult:
        self.payloads.append(payload)
        if self._error is not None:
            raise self._error

        if self._result is None:
            raise AssertionError('fake webhook result is required')

        return self._result


def destination() -> ActivationDestination:
    return ActivationDestination(
        DestinationType.WEBHOOK, 'https://example.test/webhook'
    )


def saved_job(
    job_id: str = 'job-1',
    segment_id: SegmentId = SegmentId.NEW_USER,
    status: ActivationJobStatus = ActivationJobStatus.SUCCEEDED,
) -> ActivationJob:
    return ActivationJob(
        job_id=job_id,
        segment_id=segment_id,
        destination=destination(),
        requested_at=NOW,
        status=status,
        requested_by='operator@example.com',
    )


def saved_delivery(job_id: str = 'job-1') -> ActivationDelivery:
    return ActivationDelivery(
        delivery_id='delivery-1',
        job_id=job_id,
        segment_id=SegmentId.NEW_USER,
        destination=destination(),
        attempt_number=1,
        members_count=1,
        status=ActivationDeliveryStatus.SUCCEEDED,
        requested_at=NOW,
        completed_at=NOW,
        response_status_code=204,
    )


def profile(customer_id: str = 'customer-1') -> CustomerProfile:
    return CustomerProfile(
        customer_id=customer_id,
        first_seen_at=NOW,
        last_seen_at=NOW,
        created_at=NOW,
        updated_at=NOW,
        identifiers=CustomerIdentifiers(emails=('alice@example.com',)),
        recent_events=(RecentEvent('event-1', EventType.PAGE_VIEW, NOW),),
    )


def activation_service(
    state: FakeExportState,
    segment_repository: FakeSegmentRepository,
    gateway: FakeWebhookGateway,
) -> ActivationService:
    return ActivationService(
        job_repository=FakeActivationJobRepository(state),
        delivery_repository=FakeActivationDeliveryRepository(state),
        segment_repository=segment_repository,
        export_unit_of_work=FakeExportUnitOfWork(state),
        webhook_gateway=gateway,
        job_id_generator=lambda: 'job-1',
        delivery_id_generator=lambda: 'delivery-1',
        now_provider=lambda: NOW,
    )


def run_export(
    state: FakeExportState,
    segment_repository: FakeSegmentRepository,
    gateway: FakeWebhookGateway,
):
    service = activation_service(state, segment_repository, gateway)
    return asyncio.run(
        service.export_segment(
            segment_id=SegmentId.NEW_USER,
            destination=destination(),
            requested_by='operator@example.com',
            actor_context={'trace_id': 'trace-1'},
        )
    )


def test_export_empty_segment_succeeds() -> None:
    state = FakeExportState()
    gateway = FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204))

    result = run_export(state, FakeSegmentRepository(members=()), gateway)

    assert result.job.status == ActivationJobStatus.SUCCEEDED
    assert result.job.members_count == 0
    assert result.delivery is not None
    assert result.delivery.status == ActivationDeliveryStatus.SUCCEEDED
    assert result.delivery.members_count == 0
    assert gateway.payloads[0].members == ()
    assert gateway.payloads[0].members_count == 0
    assert [job.status for job in state.job_saves] == [
        ActivationJobStatus.PENDING,
        ActivationJobStatus.RUNNING,
        ActivationJobStatus.SUCCEEDED,
    ]


def test_export_destination_timeout_fails_job() -> None:
    state = FakeExportState()
    gateway = FakeWebhookGateway(error=UseCaseDependencyError('timeout'))

    result = run_export(state, FakeSegmentRepository(members=(profile(),)), gateway)

    assert result.job.status == ActivationJobStatus.FAILED
    assert result.job.error_reason == 'timeout'
    assert result.job.members_count == 1
    assert result.delivery is not None
    assert result.delivery.status == ActivationDeliveryStatus.FAILED
    assert result.delivery.error_reason == 'timeout'


def test_export_destination_non_2xx_fails_job() -> None:
    state = FakeExportState()
    gateway = FakeWebhookGateway(
        OutboundWebhookDeliveryResult.failed(
            error_reason='destination returned non-2xx',
            response_status_code=500,
        )
    )

    result = run_export(state, FakeSegmentRepository(members=(profile(),)), gateway)

    assert result.job.status == ActivationJobStatus.FAILED
    assert result.job.error_reason == 'destination returned non-2xx'
    assert result.delivery is not None
    assert result.delivery.status == ActivationDeliveryStatus.FAILED
    assert result.delivery.response_status_code == 500
    assert result.delivery.error_reason == 'destination returned non-2xx'


def test_list_export_jobs_filters_by_segment_and_status() -> None:
    state = FakeExportState()
    expected = saved_job(status=ActivationJobStatus.SUCCEEDED)
    state.jobs[expected.job_id] = expected
    state.jobs['job-2'] = saved_job(
        job_id='job-2',
        segment_id=SegmentId.VIP,
        status=ActivationJobStatus.FAILED,
    )
    service = activation_service(
        state,
        FakeSegmentRepository(members=()),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    result = asyncio.run(
        service.list_export_jobs(
            limit=10,
            offset=0,
            segment_id=SegmentId.NEW_USER,
            status=ActivationJobStatus.SUCCEEDED,
        )
    )

    assert result.jobs == (expected,)
    assert result.total == 1
    assert result.limit == 10
    assert result.offset == 0


def test_get_export_job_returns_job_and_delivery() -> None:
    state = FakeExportState()
    job = saved_job()
    delivery = saved_delivery(job.job_id)
    state.jobs[job.job_id] = job
    state.deliveries[job.job_id] = delivery
    service = activation_service(
        state,
        FakeSegmentRepository(members=()),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    result = asyncio.run(service.get_export_job(job.job_id))

    assert result.job == job
    assert result.delivery == delivery


def test_get_export_job_missing_raises_not_found() -> None:
    service = activation_service(
        FakeExportState(),
        FakeSegmentRepository(members=()),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    with pytest.raises(ExportJobNotFoundUseCaseError):
        asyncio.run(service.get_export_job('missing-job'))


def test_export_missing_segment_raises_before_saving_job() -> None:
    state = FakeExportState()
    service = activation_service(
        state,
        FakeSegmentRepository(members=(), definition=None),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    with pytest.raises(SegmentNotFoundUseCaseError):
        asyncio.run(service.export_segment(SegmentId.NEW_USER, destination()))

    assert state.job_saves == []
    assert state.delivery_saves == []


def test_export_disabled_segment_raises_before_saving_job() -> None:
    state = FakeExportState()
    service = activation_service(
        state,
        FakeSegmentRepository(
            members=(),
            definition=SegmentDefinition(
                SegmentId.NEW_USER,
                'New User',
                'New User',
                is_active=False,
            ),
        ),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    with pytest.raises(SegmentDisabledUseCaseError):
        asyncio.run(service.export_segment(SegmentId.NEW_USER, destination()))

    assert state.job_saves == []
    assert state.delivery_saves == []


def test_export_invalid_requested_by_raises_validation_error() -> None:
    state = FakeExportState()
    service = activation_service(
        state,
        FakeSegmentRepository(members=()),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    with pytest.raises(UseCaseValidationError):
        asyncio.run(
            service.export_segment(
                SegmentId.NEW_USER,
                destination(),
                requested_by=' ',
            )
        )

    assert state.job_saves == []


def test_export_save_failure_rolls_back_and_reraises() -> None:
    state = FakeExportState(save_job_errors_remaining=1)
    service = activation_service(
        state,
        FakeSegmentRepository(members=()),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    with pytest.raises(UseCaseDependencyError):
        asyncio.run(service.export_segment(SegmentId.NEW_USER, destination()))

    assert state.rollbacks == 1
    assert state.job_saves == []


def test_export_member_loading_error_marks_running_job_failed() -> None:
    state = FakeExportState()
    service = activation_service(
        state,
        FailingMemberSegmentRepository(members=(profile(),)),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    with pytest.raises(RuntimeError, match='members unavailable'):
        asyncio.run(service.export_segment(SegmentId.NEW_USER, destination()))

    assert [job.status for job in state.job_saves] == [
        ActivationJobStatus.PENDING,
        ActivationJobStatus.RUNNING,
        ActivationJobStatus.FAILED,
    ]
    assert state.job_saves[-1].error_reason == 'members unavailable'


def test_export_blank_delivery_dependency_reason_uses_default_reason() -> None:
    state = FakeExportState()
    gateway = FakeWebhookGateway(error=UseCaseDependencyError(' '))

    result = run_export(state, FakeSegmentRepository(members=(profile(),)), gateway)

    assert result.job.status == ActivationJobStatus.FAILED
    assert result.job.error_reason == 'delivery dependency failed'
    assert result.delivery is not None
    assert result.delivery.error_reason == 'delivery dependency failed'


def test_export_final_state_save_failure_rolls_back_and_reraises() -> None:
    state = FakeExportState(save_delivery_errors_remaining=1)
    service = activation_service(
        state,
        FakeSegmentRepository(members=(profile(),)),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    with pytest.raises(UseCaseDependencyError):
        asyncio.run(service.export_segment(SegmentId.NEW_USER, destination()))

    assert state.rollbacks == 1
    assert state.job_saves[-1].status == ActivationJobStatus.FAILED
    assert state.job_saves[-1].error_reason == 'delivery repository unavailable'


def test_export_failed_final_state_save_is_retried_before_reraising() -> None:
    state = FakeExportState(save_delivery_errors_remaining=1)
    service = activation_service(
        state,
        FakeSegmentRepository(members=(profile(),)),
        FakeWebhookGateway(
            OutboundWebhookDeliveryResult.failed(
                error_reason='destination returned non-2xx',
                response_status_code=500,
            )
        ),
    )

    with pytest.raises(UseCaseDependencyError):
        asyncio.run(service.export_segment(SegmentId.NEW_USER, destination()))

    assert state.rollbacks == 1
    assert state.delivery_saves[-1].status == ActivationDeliveryStatus.FAILED
    assert state.job_saves[-1].status == ActivationJobStatus.FAILED
    assert state.job_saves[-1].error_reason == 'destination returned non-2xx'


def test_export_blank_member_loading_error_uses_exception_class_name() -> None:
    state = FakeExportState()
    service = activation_service(
        state,
        BlankFailingMemberSegmentRepository(members=(profile(),)),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    with pytest.raises(RuntimeError):
        asyncio.run(service.export_segment(SegmentId.NEW_USER, destination()))

    assert state.job_saves[-1].status == ActivationJobStatus.FAILED
    assert state.job_saves[-1].error_reason == 'RuntimeError'


def test_export_invalid_delivery_id_marks_job_failed_and_reraises() -> None:
    state = FakeExportState()
    service = ActivationService(
        job_repository=FakeActivationJobRepository(state),
        delivery_repository=FakeActivationDeliveryRepository(state),
        segment_repository=FakeSegmentRepository(members=(profile(),)),
        export_unit_of_work=FakeExportUnitOfWork(state),
        webhook_gateway=FakeWebhookGateway(
            OutboundWebhookDeliveryResult.succeeded(204)
        ),
        job_id_generator=lambda: 'job-1',
        delivery_id_generator=lambda: '',
        now_provider=lambda: NOW,
    )

    with pytest.raises(UseCaseValidationError):
        asyncio.run(service.export_segment(SegmentId.NEW_USER, destination()))

    assert state.job_saves[-1].status == ActivationJobStatus.FAILED
    assert state.job_saves[-1].error_reason == 'delivery_id must be non-empty'


def test_export_rollback_failure_is_ignored_after_save_failure() -> None:
    state = FakeExportState(
        save_job_errors_remaining=1,
        rollback_errors_remaining=1,
    )
    service = activation_service(
        state,
        FakeSegmentRepository(members=()),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    with pytest.raises(UseCaseDependencyError):
        asyncio.run(service.export_segment(SegmentId.NEW_USER, destination()))

    assert state.rollbacks == 0


def test_export_mark_failed_dependency_error_is_ignored() -> None:
    state = FakeExportState(save_job_error_on_call=3)
    service = activation_service(
        state,
        FailingMemberSegmentRepository(members=(profile(),)),
        FakeWebhookGateway(OutboundWebhookDeliveryResult.succeeded(204)),
    )

    with pytest.raises(RuntimeError, match='members unavailable'):
        asyncio.run(service.export_segment(SegmentId.NEW_USER, destination()))

    assert [job.status for job in state.job_saves] == [
        ActivationJobStatus.PENDING,
        ActivationJobStatus.RUNNING,
    ]
