from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from domain.export_job import (
    ActivationDelivery,
    ActivationDeliveryStatus,
    ActivationDestination,
    ActivationJob,
    ActivationJobStatus,
    DestinationType,
)
from domain.segment import SegmentId
from usecase.error import UseCaseDependencyError

_JOB_COLUMNS = """
    job_id, segment_id, destination_type, destination_url, status,
    members_count, requested_by_user_id, requested_at, completed_at, error_reason
"""

_JOB_UPSERT_SQL = """
    INSERT INTO activation_jobs (
        job_id, segment_id, destination_type, destination_url, status,
        members_count, requested_by_user_id, requested_by_email, requested_at,
        started_at, completed_at, error_reason, created_at, updated_at
    ) VALUES (
        :job_id, :segment_id, :destination_type, :destination_url, :status,
        :members_count, :requested_by_user_id, NULL, :requested_at,
        NULL, :completed_at, :error_reason, :created_at, :updated_at
    )
    ON CONFLICT (job_id) DO UPDATE SET
        status        = EXCLUDED.status,
        members_count = EXCLUDED.members_count,
        completed_at  = EXCLUDED.completed_at,
        error_reason  = EXCLUDED.error_reason,
        updated_at    = EXCLUDED.updated_at
"""

_DELIVERY_UPSERT_SQL = """
    INSERT INTO activation_deliveries (
        delivery_id, job_id, attempt_number, status,
        request_payload_json, response_status_code, error_reason,
        started_at, completed_at, created_at
    ) VALUES (
        :delivery_id, :job_id, :attempt_number, :status,
        NULL, :response_status_code, :error_reason,
        :started_at, :completed_at, :created_at
    )
    ON CONFLICT (delivery_id) DO UPDATE SET
        status               = EXCLUDED.status,
        response_status_code = EXCLUDED.response_status_code,
        error_reason         = EXCLUDED.error_reason,
        completed_at         = EXCLUDED.completed_at
"""

_DELIVERY_SELECT_SQL = """
    SELECT
        ad.delivery_id,
        ad.job_id,
        ad.attempt_number,
        ad.status,
        ad.response_status_code,
        ad.error_reason,
        ad.started_at,
        ad.completed_at,
        aj.segment_id,
        aj.destination_type,
        aj.destination_url,
        aj.members_count
    FROM activation_deliveries ad
    INNER JOIN activation_jobs aj ON aj.job_id = ad.job_id
    WHERE ad.job_id = :job_id
    ORDER BY ad.attempt_number DESC
    LIMIT 1
"""


class ActivationJobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_job_id(self, job_id: str) -> ActivationJob | None:
        try:
            result = await self._session.execute(
                sa.text(
                    f'SELECT {_JOB_COLUMNS} FROM activation_jobs WHERE job_id = :job_id'
                ),
                {'job_id': job_id},
            )
        except Exception as exc:
            raise UseCaseDependencyError('activation job lookup failed') from exc

        row = result.mappings().first()
        if row is None:
            return None
        return _row_to_job(row)

    async def list_jobs(
        self,
        limit: int = 50,
        offset: int = 0,
        segment_id: SegmentId | None = None,
        status: ActivationJobStatus | None = None,
    ) -> tuple[ActivationJob, ...]:
        where, filter_params = _jobs_where(segment_id, status)
        sql = f"""
            SELECT {_JOB_COLUMNS}
            FROM activation_jobs
            {where}
            ORDER BY requested_at DESC
            LIMIT :limit OFFSET :offset
        """
        params: dict[str, Any] = {**filter_params, 'limit': limit, 'offset': offset}

        try:
            result = await self._session.execute(sa.text(sql), params)
        except Exception as exc:
            raise UseCaseDependencyError('activation jobs list failed') from exc

        return tuple(_row_to_job(row) for row in result.mappings().all())

    async def count_jobs(
        self,
        limit: int = 50,
        offset: int = 0,
        segment_id: SegmentId | None = None,
        status: ActivationJobStatus | None = None,
    ) -> int:
        where, filter_params = _jobs_where(segment_id, status)
        sql = f'SELECT COUNT(*) FROM activation_jobs\n{where}'

        try:
            result = await self._session.execute(sa.text(sql), filter_params)
        except Exception as exc:
            raise UseCaseDependencyError('activation jobs count failed') from exc

        return result.scalar() or 0

    async def save_job(self, job: ActivationJob) -> None:
        try:
            await self._session.execute(sa.text(_JOB_UPSERT_SQL), _job_to_params(job))
        except Exception as exc:
            raise UseCaseDependencyError('activation job save failed') from exc


class ActivationDeliveryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_for_job(self, job_id: str) -> ActivationDelivery | None:
        try:
            result = await self._session.execute(
                sa.text(_DELIVERY_SELECT_SQL),
                {'job_id': job_id},
            )
        except Exception as exc:
            raise UseCaseDependencyError('activation delivery lookup failed') from exc

        row = result.mappings().first()
        if row is None:
            return None
        return _row_to_delivery(row)

    async def save_delivery(self, delivery: ActivationDelivery) -> None:
        try:
            await self._session.execute(
                sa.text(_DELIVERY_UPSERT_SQL),
                _delivery_to_params(delivery),
            )
        except Exception as exc:
            raise UseCaseDependencyError('activation delivery save failed') from exc


def _jobs_where(
    segment_id: SegmentId | None,
    status: ActivationJobStatus | None,
) -> tuple[str, dict[str, Any]]:
    conditions: list[str] = []
    params: dict[str, Any] = {}

    if segment_id is not None:
        conditions.append('segment_id = :filter_segment_id')
        params['filter_segment_id'] = segment_id.value

    if status is not None:
        conditions.append('status = :filter_status')
        params['filter_status'] = status.value

    if not conditions:
        return '', params
    return 'WHERE ' + ' AND '.join(conditions), params


def _job_to_params(job: ActivationJob) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        'job_id': job.job_id,
        'segment_id': job.segment_id.value,
        'destination_type': job.destination.destination_type.value,
        'destination_url': job.destination.url,
        'status': job.status.value,
        'members_count': job.members_count,
        'requested_by_user_id': job.requested_by,
        'requested_at': job.requested_at,
        'completed_at': job.completed_at,
        'error_reason': job.error_reason,
        'created_at': job.requested_at,
        'updated_at': now,
    }


def _delivery_to_params(delivery: ActivationDelivery) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        'delivery_id': delivery.delivery_id,
        'job_id': delivery.job_id,
        'attempt_number': delivery.attempt_number,
        'status': delivery.status.value,
        'response_status_code': delivery.response_status_code,
        'error_reason': delivery.error_reason,
        'started_at': delivery.requested_at,
        'completed_at': delivery.completed_at,
        'created_at': now,
    }


def _row_to_job(row: Any) -> ActivationJob:
    return ActivationJob(
        job_id=row['job_id'],
        segment_id=SegmentId(row['segment_id']),
        destination=ActivationDestination(
            destination_type=DestinationType(row['destination_type']),
            url=row['destination_url'],
        ),
        requested_at=row['requested_at'],
        status=ActivationJobStatus(row['status']),
        members_count=row['members_count'],
        requested_by=row['requested_by_user_id'],
        completed_at=row['completed_at'],
        error_reason=row['error_reason'],
    )


def _row_to_delivery(row: Any) -> ActivationDelivery:
    return ActivationDelivery(
        delivery_id=row['delivery_id'],
        job_id=row['job_id'],
        segment_id=SegmentId(row['segment_id']),
        destination=ActivationDestination(
            destination_type=DestinationType(row['destination_type']),
            url=row['destination_url'],
        ),
        attempt_number=row['attempt_number'],
        members_count=row['members_count'],
        status=ActivationDeliveryStatus(row['status']),
        requested_at=row['started_at'],
        completed_at=row['completed_at'],
        response_status_code=row['response_status_code'],
        error_reason=row['error_reason'],
    )
