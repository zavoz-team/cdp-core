from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime

from domain.error import DomainError
from domain.export_job import (
    ActivationDelivery,
    ActivationDeliveryStatus,
    ActivationDestination,
    ActivationJob,
    ActivationJobStatus,
)
from domain.profile import CustomerProfile
from domain.segment import SegmentId
from usecase.criteria import validate_pagination
from usecase.dto import (
    ExportSegmentResult,
    GetExportJobResult,
    ListExportJobsResult,
    OutboundWebhookDeliveryResult,
    SegmentExportPayload,
)
from usecase.error import (
    ExportJobNotFoundUseCaseError,
    SegmentDisabledUseCaseError,
    SegmentNotFoundUseCaseError,
    UseCaseDependencyError,
    UseCaseValidationError,
)
from usecase.interface import (
    ActivationDeliveryRepository,
    ActivationJobRepository,
    ExportTransaction,
    ExportUnitOfWork,
    OutboundWebhookGateway,
    SegmentRepository,
)

REASON_DELIVERY_DEPENDENCY_FAILED = 'delivery dependency failed'


class ActivationService:
    def __init__(
        self,
        job_repository: ActivationJobRepository,
        delivery_repository: ActivationDeliveryRepository,
        segment_repository: SegmentRepository,
        export_unit_of_work: ExportUnitOfWork,
        webhook_gateway: OutboundWebhookGateway,
        job_id_generator: Callable[[], str],
        delivery_id_generator: Callable[[], str],
        now_provider: Callable[[], datetime],
    ) -> None:
        self._job_repository = job_repository
        self._delivery_repository = delivery_repository
        self._segment_repository = segment_repository
        self._export_unit_of_work = export_unit_of_work
        self._webhook_gateway = webhook_gateway
        self._job_id_generator = job_id_generator
        self._delivery_id_generator = delivery_id_generator
        self._now_provider = now_provider

    async def list_export_jobs(
        self,
        limit: int = 50,
        offset: int = 0,
        segment_id: SegmentId | None = None,
        status: ActivationJobStatus | None = None,
    ) -> ListExportJobsResult:
        validate_pagination(limit, offset)
        jobs = await self._job_repository.list_jobs(
            limit=limit,
            offset=offset,
            segment_id=segment_id,
            status=status,
        )
        total = await self._job_repository.count_jobs(
            limit=limit,
            offset=offset,
            segment_id=segment_id,
            status=status,
        )

        return ListExportJobsResult(
            jobs=jobs,
            total=total,
            limit=limit,
            offset=offset,
        )

    async def get_export_job(self, job_id: str) -> GetExportJobResult:
        job = await self._job_repository.get_by_job_id(job_id)
        if job is None:
            raise ExportJobNotFoundUseCaseError(f'export job not found: {job_id}')

        delivery = await self._delivery_repository.get_for_job(job_id)
        return GetExportJobResult(job=job, delivery=delivery)

    async def export_segment(
        self,
        segment_id: SegmentId,
        destination: ActivationDestination,
        requested_by: str | None = None,
        actor_context: Mapping[str, object] | None = None,
    ) -> ExportSegmentResult:
        definition = await self._segment_repository.get_definition(segment_id)
        if definition is None:
            raise SegmentNotFoundUseCaseError(f'segment not found: {segment_id}')

        if not definition.is_active:
            raise SegmentDisabledUseCaseError(f'segment disabled: {segment_id}')

        job = _activation_job(
            job_id=self._job_id_generator(),
            segment_id=segment_id,
            destination=destination,
            requested_by=requested_by,
            actor_context=actor_context,
            requested_at=self._now_provider(),
        )
        await self._save_job(job)

        running_job = replace(job, status=ActivationJobStatus.RUNNING)
        await self._save_job(running_job)

        final_job: ActivationJob | None = None
        delivery: ActivationDelivery | None = None
        try:
            members = await self._load_members(segment_id)
            delivered_members_count = len(members)
            delivered_at = self._now_provider()
            delivery_result = await self._send_delivery(
                segment_id,
                destination,
                running_job.job_id,
                delivered_at,
                delivered_members_count,
                members,
            )
            completed_at = self._now_provider()
            delivery = _activation_delivery(
                delivery_id=self._delivery_id_generator(),
                segment_id=segment_id,
                destination=destination,
                job_id=running_job.job_id,
                members_count=delivered_members_count,
                requested_at=delivered_at,
                completed_at=completed_at,
                result=delivery_result,
            )
            final_job = _final_job(
                running_job,
                delivery_result,
                delivered_members_count,
                completed_at,
            )
            await self._save_final_state(final_job, delivery)
            return ExportSegmentResult(job=final_job, delivery=delivery)
        except Exception as error:
            await self._mark_failed_after_running_error(
                running_job,
                final_job,
                delivery,
                error,
            )
            raise

    async def _load_members(self, segment_id: SegmentId) -> tuple[CustomerProfile, ...]:
        members_count = await self._segment_repository.count_members(segment_id)
        if members_count == 0:
            return ()

        return await self._segment_repository.list_member_profiles(
            segment_id,
            members_count,
            0,
        )

    async def _send_delivery(
        self,
        segment_id: SegmentId,
        destination: ActivationDestination,
        job_id: str,
        exported_at: datetime,
        members_count: int,
        members: tuple[CustomerProfile, ...],
    ) -> OutboundWebhookDeliveryResult:
        payload = SegmentExportPayload(
            job_id=job_id,
            segment_id=segment_id,
            exported_at=exported_at,
            members_count=members_count,
            members=members,
        )
        try:
            return await self._webhook_gateway.send_segment_export(
                destination,
                payload,
            )
        except UseCaseDependencyError as error:
            return OutboundWebhookDeliveryResult.failed(
                error_reason=_dependency_error_reason(error)
            )

    async def _save_job(self, job: ActivationJob) -> None:
        transaction = await self._export_unit_of_work.begin()
        committed = False
        try:
            await transaction.jobs.save_job(job)
            await transaction.commit()
            committed = True
        finally:
            if not committed:
                await self._rollback(transaction)

    async def _save_final_state(
        self,
        job: ActivationJob,
        delivery: ActivationDelivery,
    ) -> None:
        transaction = await self._export_unit_of_work.begin()
        committed = False
        try:
            await transaction.deliveries.save_delivery(delivery)
            await transaction.jobs.save_job(job)
            await transaction.commit()
            committed = True
        finally:
            if not committed:
                await self._rollback(transaction)

    async def _mark_failed_after_running_error(
        self,
        running_job: ActivationJob,
        final_job: ActivationJob | None,
        delivery: ActivationDelivery | None,
        error: Exception,
    ) -> None:
        try:
            if final_job is not None and final_job.status == ActivationJobStatus.FAILED:
                if delivery is None:
                    await self._save_job(final_job)
                else:
                    await self._save_final_state(final_job, delivery)
                return

            failed_job = _failed_job_from_error(
                running_job,
                _exception_error_reason(error),
                self._now_provider(),
            )
            await self._save_job(failed_job)
        except UseCaseDependencyError:
            pass

    async def _rollback(self, transaction: ExportTransaction) -> None:
        try:
            await transaction.rollback()
        except UseCaseDependencyError:
            pass


def _activation_job(
    job_id: str,
    segment_id: SegmentId,
    destination: ActivationDestination,
    requested_by: str | None,
    actor_context: Mapping[str, object] | None,
    requested_at: datetime,
) -> ActivationJob:
    try:
        return ActivationJob(
            job_id=job_id,
            segment_id=segment_id,
            destination=destination,
            requested_at=requested_at,
            status=ActivationJobStatus.PENDING,
            requested_by=requested_by,
            actor_context=actor_context or {},
        )
    except DomainError as error:
        raise UseCaseValidationError(str(error)) from error


def _activation_delivery(
    delivery_id: str,
    segment_id: SegmentId,
    destination: ActivationDestination,
    job_id: str,
    members_count: int,
    requested_at: datetime,
    completed_at: datetime,
    result: OutboundWebhookDeliveryResult,
) -> ActivationDelivery:
    try:
        return ActivationDelivery(
            delivery_id=delivery_id,
            job_id=job_id,
            segment_id=segment_id,
            destination=destination,
            attempt_number=1,
            members_count=members_count,
            status=_delivery_status(result),
            requested_at=requested_at,
            completed_at=completed_at,
            response_status_code=result.response_status_code,
            error_reason=result.error_reason,
        )
    except DomainError as error:
        raise UseCaseValidationError(str(error)) from error


def _delivery_status(
    result: OutboundWebhookDeliveryResult,
) -> ActivationDeliveryStatus:
    if result.delivered:
        return ActivationDeliveryStatus.SUCCEEDED

    return ActivationDeliveryStatus.FAILED


def _final_job(
    running_job: ActivationJob,
    result: OutboundWebhookDeliveryResult,
    members_count: int,
    completed_at: datetime,
) -> ActivationJob:
    if result.delivered:
        return replace(
            running_job,
            status=ActivationJobStatus.SUCCEEDED,
            members_count=members_count,
            completed_at=completed_at,
            error_reason=None,
        )

    return replace(
        running_job,
        status=ActivationJobStatus.FAILED,
        members_count=members_count,
        completed_at=completed_at,
        error_reason=result.error_reason,
    )


def _failed_job_from_error(
    running_job: ActivationJob,
    error_reason: str,
    completed_at: datetime,
) -> ActivationJob:
    return replace(
        running_job,
        status=ActivationJobStatus.FAILED,
        completed_at=completed_at,
        error_reason=error_reason,
    )


def _exception_error_reason(error: Exception) -> str:
    reason = str(error).strip()
    if reason:
        return reason

    return error.__class__.__name__


def _dependency_error_reason(error: UseCaseDependencyError) -> str:
    reason = str(error).strip()
    if reason:
        return reason

    return REASON_DELIVERY_DEPENDENCY_FAILED
