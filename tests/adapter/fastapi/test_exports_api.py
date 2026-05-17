import os
from collections.abc import Mapping
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
)
from domain.segment import SegmentId
from usecase.dto import ExportSegmentResult
from usecase.error import SegmentDisabledUseCaseError
from usecase.export import ActivationService


class MockActivationService:
    async def export_segment(
        self,
        segment_id: SegmentId,
        destination: ActivationDestination,
        requested_by: str | None = None,
        actor_context: Mapping[str, object] | None = None,
    ) -> ExportSegmentResult:
        if segment_id == SegmentId.ACTIVE:
            raise SegmentDisabledUseCaseError(f'segment disabled: {segment_id}')

        if segment_id == SegmentId.NEW_USER:  # Simulate empty segment succeeds
            job = ActivationJob(
                job_id='job_empty',
                segment_id=segment_id,
                destination=destination,
                requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                status=ActivationJobStatus.SUCCEEDED,
                members_count=0,
                requested_by=requested_by,
                actor_context=actor_context or {},
                completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            delivery = ActivationDelivery(
                delivery_id='del_empty',
                job_id='job_empty',
                segment_id=segment_id,
                destination=destination,
                attempt_number=1,
                members_count=0,
                status=ActivationDeliveryStatus.SUCCEEDED,
                requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                response_status_code=200,
            )
            return ExportSegmentResult(job=job, delivery=delivery)

        if 'timeout' in destination.url:
            job = ActivationJob(
                job_id='job_failed',
                segment_id=segment_id,
                destination=destination,
                requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                status=ActivationJobStatus.FAILED,
                members_count=100,
                requested_by=requested_by,
                actor_context=actor_context or {},
                completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                error_reason='Connection timeout',
            )
            delivery = ActivationDelivery(
                delivery_id='del_failed',
                job_id='job_failed',
                segment_id=segment_id,
                destination=destination,
                attempt_number=1,
                members_count=100,
                status=ActivationDeliveryStatus.FAILED,
                requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                error_reason='Connection timeout',
            )
            return ExportSegmentResult(job=job, delivery=delivery)

        # Success case
        job = ActivationJob(
            job_id='job_1',
            segment_id=segment_id,
            destination=destination,
            requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=ActivationJobStatus.SUCCEEDED,
            members_count=100,
            requested_by=requested_by,
            actor_context=actor_context or {},
            completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        delivery = ActivationDelivery(
            delivery_id='del_1',
            job_id='job_1',
            segment_id=segment_id,
            destination=destination,
            attempt_number=1,
            members_count=100,
            status=ActivationDeliveryStatus.SUCCEEDED,
            requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            response_status_code=200,
        )
        return ExportSegmentResult(job=job, delivery=delivery)


def override_activation_service() -> ActivationService:
    return cast(ActivationService, MockActivationService())


app = create_app()
app.dependency_overrides[get_stub(ActivationService)] = override_activation_service
client = TestClient(app)


def test_export_segment_requires_auth() -> None:
    response = client.post(
        '/api/v1/exports',
        json={
            'segment_id': 'vip',
            'destination_type': 'webhook',
            'destination_url': 'https://example.com/hooks/cdp',
        },
    )
    assert response.status_code == 401
    assert response.json()['detail'] == 'Missing service token'


def test_export_segment_fails_with_invalid_token() -> None:
    response = client.post(
        '/api/v1/exports',
        headers={'X-Service-Token': 'invalid-token'},
        json={
            'segment_id': 'vip',
            'destination_type': 'webhook',
            'destination_url': 'https://example.com/hooks/cdp',
        },
    )
    assert response.status_code == 403


def test_export_segment_succeeds_with_valid_token() -> None:
    os.environ['APP_API_TOKEN'] = 'valid-token'
    response = client.post(
        '/api/v1/exports',
        headers={
            'X-Service-Token': 'valid-token',
        },
        json={
            'segment_id': 'vip',
            'destination_type': 'webhook',
            'destination_url': 'https://example.com/hooks/cdp',
            'requested_by': 'operator@example.com',
            'actor_context': {'user_id': 'operator_1'},
        },
    )
    if response.status_code != 200:
        print(response.json())
    assert response.status_code == 200
    data = response.json()
    assert data['job_id'] == 'job_1'
    assert data['status'] == 'succeeded'
    assert data['members_count'] == 100
    assert data['delivery']['status'] == 'succeeded'
    assert data['delivery']['response_status_code'] == 200


def test_export_empty_segment_succeeds() -> None:
    os.environ['APP_API_TOKEN'] = 'valid-token'
    response = client.post(
        '/api/v1/exports',
        headers={'X-Service-Token': 'valid-token'},
        json={
            'segment_id': 'new_user',
            'destination_type': 'webhook',
            'destination_url': 'https://example.com/hooks/cdp',
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data['job_id'] == 'job_empty'
    assert data['status'] == 'succeeded'
    assert data['members_count'] == 0


def test_export_destination_timeout_returns_failed_job() -> None:
    os.environ['APP_API_TOKEN'] = 'valid-token'
    response = client.post(
        '/api/v1/exports',
        headers={'X-Service-Token': 'valid-token'},
        json={
            'segment_id': 'vip',
            'destination_type': 'webhook',
            'destination_url': 'https://example.com/timeout',
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data['job_id'] == 'job_failed'
    assert data['status'] == 'failed'
    assert data['error_reason'] == 'Connection timeout'
    assert data['delivery']['status'] == 'failed'


def test_export_disabled_segment_returns_stable_error() -> None:
    os.environ['APP_API_TOKEN'] = 'valid-token'
    response = client.post(
        '/api/v1/exports',
        headers={'X-Service-Token': 'valid-token'},
        json={
            'segment_id': 'active',
            'destination_type': 'webhook',
            'destination_url': 'https://example.com/hooks/cdp',
        },
    )
    assert response.status_code == 400
    assert response.json()['detail'] == 'Segment is disabled'


def test_export_unknown_segment_returns_400() -> None:
    os.environ['APP_API_TOKEN'] = 'valid-token'
    response = client.post(
        '/api/v1/exports',
        headers={'X-Service-Token': 'valid-token'},
        json={
            'segment_id': 'unknown_segment',
            'destination_type': 'webhook',
            'destination_url': 'https://example.com/hooks/cdp',
        },
    )
    assert response.status_code == 400
