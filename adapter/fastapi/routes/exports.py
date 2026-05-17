from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from adapter.fastapi.auth import verify_service_token
from adapter.fastapi.dependencies import provide
from domain.export_job import ActivationDestination, DestinationType
from domain.segment import SegmentId
from usecase.error import SegmentDisabledUseCaseError, SegmentNotFoundUseCaseError
from usecase.export import ActivationService

router = APIRouter(
    prefix='/exports',
    tags=['Exports'],
    dependencies=[Depends(verify_service_token)],
)


class ExportRequest(BaseModel):
    segment_id: str
    destination_type: str
    destination_url: str
    requested_by: str | None = None
    actor_context: dict[str, Any] | None = None


class DeliveryResponse(BaseModel):
    status: str
    requested_at: datetime
    completed_at: datetime | None
    response_status_code: int | None
    error_reason: str | None


class ExportResponse(BaseModel):
    job_id: str
    segment_id: str
    status: str
    members_count: int
    delivery: DeliveryResponse | None = None
    error_reason: str | None = None


@router.post('', response_model=ExportResponse)
async def create_export(
    request: ExportRequest,
    service: ActivationService = provide(ActivationService),
) -> ExportResponse:
    try:
        segment_id = SegmentId(request.segment_id)
        destination_type = DestinationType(request.destination_type)
        destination = ActivationDestination(
            destination_type=destination_type,
            url=request.destination_url,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        result = await service.export_segment(
            segment_id=segment_id,
            destination=destination,
            requested_by=request.requested_by,
            actor_context=request.actor_context,
        )

        delivery_response = None
        if result.delivery:
            delivery_response = DeliveryResponse(
                status=str(result.delivery.status),
                requested_at=result.delivery.requested_at,
                completed_at=result.delivery.completed_at,
                response_status_code=result.delivery.response_status_code,
                error_reason=result.delivery.error_reason,
            )

        return ExportResponse(
            job_id=result.job.job_id,
            segment_id=str(result.job.segment_id),
            status=str(result.job.status),
            members_count=result.job.members_count,
            delivery=delivery_response,
            error_reason=result.job.error_reason,
        )

    except SegmentNotFoundUseCaseError:
        raise HTTPException(status_code=404, detail='Segment not found')
    except SegmentDisabledUseCaseError:
        raise HTTPException(status_code=400, detail='Segment is disabled')
