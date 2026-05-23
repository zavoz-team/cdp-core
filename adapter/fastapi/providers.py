import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timezone

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from adapter.fastapi.provider_registry import registry
from adapter.fastapi.webhook_payload import HttpxWebhookGateway
from repository.activation import ActivationDeliveryRepository, ActivationJobRepository
from repository.customer_profile import CustomerProfileRepository
from repository.segment import SegmentRepository
from repository.uow import SqlAlchemyExportUnitOfWork
from usecase.export import ActivationService
from usecase.interface import Logger, Metrics, Tracer
from usecase.profile import ProfileService
from usecase.segment import SegmentService


async def get_db_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    session_factory = request.app.state.session_factory
    async with session_factory() as session:
        yield session


def _get_obs(request: Request) -> tuple[Logger, Tracer, Metrics]:
    obs = request.app.state.obs
    return obs.logger, obs.tracer, obs.metrics


@registry.register(ProfileService)
def provide_profile_service(
    request: Request,
    session: AsyncSession = Depends(get_db_session),
) -> ProfileService:
    logger, tracer, _ = _get_obs(request)
    repo = CustomerProfileRepository(session, logger, tracer)
    return ProfileService(repo)


@registry.register(SegmentService)
def provide_segment_service(
    request: Request,
    session: AsyncSession = Depends(get_db_session),
) -> SegmentService:
    logger, tracer, _ = _get_obs(request)
    repo = SegmentRepository(session, logger, tracer)
    return SegmentService(repo)


def get_job_id() -> str:
    return f'job_{uuid.uuid4().hex}'


def get_delivery_id() -> str:
    return f'del_{uuid.uuid4().hex}'


def get_now() -> datetime:
    return datetime.now(timezone.utc)


@registry.register(ActivationService)
def provide_activation_service(
    request: Request,
    session: AsyncSession = Depends(get_db_session),
) -> ActivationService:
    logger, tracer, metrics = _get_obs(request)
    job_repo = ActivationJobRepository(session, logger, tracer)
    delivery_repo = ActivationDeliveryRepository(session, logger, tracer)
    segment_repo = SegmentRepository(session, logger, tracer)
    uow = SqlAlchemyExportUnitOfWork(request.app.state.session_factory, logger, tracer)
    gateway = HttpxWebhookGateway(request.app.state.http_client)

    return ActivationService(
        job_repository=job_repo,
        delivery_repository=delivery_repo,
        segment_repository=segment_repo,
        export_unit_of_work=uow,
        webhook_gateway=gateway,
        job_id_generator=get_job_id,
        delivery_id_generator=get_delivery_id,
        now_provider=get_now,
        logger=logger,
        tracer=tracer,
        metrics=metrics,
    )
