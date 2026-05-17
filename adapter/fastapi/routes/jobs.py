from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from adapter.fastapi.dependencies import provide
from domain.export_job import ActivationJobStatus
from domain.segment import SegmentId
from usecase.error import ExportJobNotFoundUseCaseError
from usecase.export import ActivationService

router = APIRouter(prefix='/jobs', tags=['Jobs'])


class JobSummaryResponse(BaseModel):
    job_id: str
    segment_id: str
    status: str
    members_count: int
    requested_by: str | None
    requested_at: datetime
    completed_at: datetime | None


class JobListResponse(BaseModel):
    items: list[JobSummaryResponse]
    total: int
    limit: int
    offset: int


class DeliveryResponse(BaseModel):
    status: str
    requested_at: datetime
    completed_at: datetime | None
    response_status_code: int | None
    error_reason: str | None


class JobDetailsResponse(BaseModel):
    job: JobSummaryResponse
    delivery: DeliveryResponse | None = None
    error_reason: str | None = None


@router.get('', response_model=JobListResponse)
async def list_jobs(
    limit: int = 50,
    offset: int = 0,
    segment_id: str | None = None,
    status: str | None = None,
    service: ActivationService = provide(ActivationService),
) -> JobListResponse:
    parsed_segment_id = None
    if segment_id:
        try:
            parsed_segment_id = SegmentId(segment_id)
        except ValueError:
            raise HTTPException(status_code=400, detail='Invalid segment_id')

    parsed_status = None
    if status:
        try:
            parsed_status = ActivationJobStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail='Invalid status')

    result = await service.list_export_jobs(
        limit=limit,
        offset=offset,
        segment_id=parsed_segment_id,
        status=parsed_status,
    )

    return JobListResponse(
        items=[
            JobSummaryResponse(
                job_id=job.job_id,
                segment_id=str(job.segment_id),
                status=str(job.status),
                members_count=job.members_count,
                requested_by=job.requested_by,
                requested_at=job.requested_at,
                completed_at=job.completed_at,
            )
            for job in result.jobs
        ],
        total=result.total,
        limit=result.limit,
        offset=result.offset,
    )


@router.get('/{job_id}', response_model=JobDetailsResponse)
async def get_job(
    job_id: str,
    service: ActivationService = provide(ActivationService),
) -> JobDetailsResponse:
    try:
        result = await service.get_export_job(job_id)

        delivery_response = None
        if result.delivery:
            delivery_response = DeliveryResponse(
                status=str(result.delivery.status),
                requested_at=result.delivery.requested_at,
                completed_at=result.delivery.completed_at,
                response_status_code=result.delivery.response_status_code,
                error_reason=result.delivery.error_reason,
            )

        return JobDetailsResponse(
            job=JobSummaryResponse(
                job_id=result.job.job_id,
                segment_id=str(result.job.segment_id),
                status=str(result.job.status),
                members_count=result.job.members_count,
                requested_by=result.job.requested_by,
                requested_at=result.job.requested_at,
                completed_at=result.job.completed_at,
            ),
            delivery=delivery_response,
            error_reason=result.job.error_reason,
        )
    except ExportJobNotFoundUseCaseError:
        raise HTTPException(status_code=404, detail='Job not found')
