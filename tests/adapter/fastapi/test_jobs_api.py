from datetime import datetime, timezone
from typing import cast

from fastapi.testclient import TestClient

from adapter.fastapi.app import create_app
from adapter.fastapi.dependencies import get_stub
from domain.export_job import (
    ActivationDelivery,
    ActivationDeliveryStatus,
    ActivationDestination,
    ActivationJob,
    ActivationJobStatus,
    DestinationType,
)
from domain.segment import SegmentId
from usecase.dto import GetExportJobResult, ListExportJobsResult
from usecase.error import ExportJobNotFoundUseCaseError
from usecase.export import ActivationService


class MockActivationServiceForJobs:
    async def list_export_jobs(
        self,
        limit: int = 50,
        offset: int = 0,
        segment_id: SegmentId | None = None,
        status: ActivationJobStatus | None = None,
    ) -> ListExportJobsResult:
        job = ActivationJob(
            job_id='job_1',
            segment_id=segment_id or SegmentId.VIP,
            destination=ActivationDestination(
                destination_type=DestinationType.WEBHOOK, url='http://example.com'
            ),
            requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=status or ActivationJobStatus.SUCCEEDED,
            members_count=100,
            requested_by='operator@example.com',
            actor_context={'user_id': 'operator_1'},
            completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        return ListExportJobsResult(
            jobs=(job,),
            total=1,
            limit=limit,
            offset=offset,
        )

    async def get_export_job(self, job_id: str) -> GetExportJobResult:
        if job_id == 'unknown':
            raise ExportJobNotFoundUseCaseError(f'export job not found: {job_id}')

        job = ActivationJob(
            job_id=job_id,
            segment_id=SegmentId.VIP,
            destination=ActivationDestination(
                destination_type=DestinationType.WEBHOOK, url='http://example.com'
            ),
            requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=ActivationJobStatus.FAILED,
            members_count=100,
            requested_by='operator@example.com',
            actor_context={'user_id': 'operator_1'},
            completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            error_reason='Some error',
        )

        delivery = ActivationDelivery(
            delivery_id='del_1',
            job_id=job_id,
            segment_id=SegmentId.VIP,
            destination=ActivationDestination(
                destination_type=DestinationType.WEBHOOK, url='http://example.com'
            ),
            attempt_number=1,
            members_count=100,
            status=ActivationDeliveryStatus.FAILED,
            requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            response_status_code=500,
            error_reason='Some delivery error',
        )

        return GetExportJobResult(job=job, delivery=delivery)


def override_activation_service() -> ActivationService:
    return cast(ActivationService, MockActivationServiceForJobs())


app = create_app()
app.dependency_overrides[get_stub(ActivationService)] = override_activation_service
client = TestClient(app)


def test_list_jobs_returns_200_and_supports_pagination_and_filters() -> None:
    response = client.get(
        '/api/v1/jobs?limit=10&offset=0&segment_id=vip&status=succeeded'
    )

    assert response.status_code == 200
    data = response.json()
    assert data['total'] == 1
    assert data['limit'] == 10
    assert data['offset'] == 0

    items = data['items']
    assert len(items) == 1
    assert items[0]['job_id'] == 'job_1'
    assert items[0]['segment_id'] == 'vip'
    assert items[0]['status'] == 'succeeded'
    assert items[0]['members_count'] == 100
    assert items[0]['requested_by'] == 'operator@example.com'
    assert 'requested_at' in items[0]


def test_list_jobs_returns_400_for_invalid_segment_id() -> None:
    response = client.get('/api/v1/jobs?segment_id=invalid')
    assert response.status_code == 400
    assert response.json()['detail'] == 'Invalid segment_id'


def test_list_jobs_returns_400_for_invalid_status() -> None:
    response = client.get('/api/v1/jobs?status=invalid')
    assert response.status_code == 400
    assert response.json()['detail'] == 'Invalid status'


def test_get_job_returns_200_with_delivery_result() -> None:
    response = client.get('/api/v1/jobs/job_1')

    assert response.status_code == 200
    data = response.json()

    assert data['job']['job_id'] == 'job_1'
    assert data['job']['segment_id'] == 'vip'
    assert data['job']['status'] == 'failed'

    assert data['delivery']['status'] == 'failed'
    assert data['delivery']['response_status_code'] == 500
    assert data['delivery']['error_reason'] == 'Some delivery error'

    assert data['error_reason'] == 'Some error'


def test_get_job_returns_404_for_unknown_job() -> None:
    response = client.get('/api/v1/jobs/unknown')

    assert response.status_code == 404
    assert response.json()['detail'] == 'Job not found'
