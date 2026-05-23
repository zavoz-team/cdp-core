from collections.abc import Callable

from sqlalchemy.ext.asyncio import AsyncSession

from repository.activation import ActivationDeliveryRepository, ActivationJobRepository
from repository.customer_profile import CustomerProfileRepository
from repository.identity import IdentityRepository
from repository.purchase import PurchaseRepository
from repository.raw_event import RawEventRepository
from repository.segment import SegmentRepository
from usecase.error import UseCaseDependencyError
from usecase.interface import (
    EventProcessingTransaction,
    EventProcessingUnitOfWork,
    ExportTransaction,
    ExportUnitOfWork,
    Logger,
    Tracer,
)


class SqlAlchemyExportTransaction(ExportTransaction):
    def __init__(
        self, session: AsyncSession, logger: Logger, tracer: Tracer
    ) -> None:
        self._session = session
        self._logger = logger
        self._tracer = tracer
        self.jobs = ActivationJobRepository(session, logger, tracer)
        self.deliveries = ActivationDeliveryRepository(session, logger, tracer)

    async def commit(self) -> None:
        with self._tracer.start_span('repo.export_transaction.commit'):
            try:
                await self._session.commit()
            except Exception as exc:
                raise UseCaseDependencyError('commit failed') from exc
            finally:
                await self._session.close()

    async def rollback(self) -> None:
        with self._tracer.start_span('repo.export_transaction.rollback'):
            try:
                await self._session.rollback()
            except Exception as exc:
                raise UseCaseDependencyError('rollback failed') from exc
            finally:
                await self._session.close()


class SqlAlchemyExportUnitOfWork(ExportUnitOfWork):
    def __init__(
        self,
        session_factory: Callable[[], AsyncSession],
        logger: Logger,
        tracer: Tracer,
    ) -> None:
        self._session_factory = session_factory
        self._logger = logger
        self._tracer = tracer

    async def begin(self) -> ExportTransaction:
        session = self._session_factory()
        return SqlAlchemyExportTransaction(session, self._logger, self._tracer)


class SqlAlchemyEventProcessingTransaction(EventProcessingTransaction):
    def __init__(
        self, session: AsyncSession, logger: Logger, tracer: Tracer
    ) -> None:
        self._session = session
        self._logger = logger
        self._tracer = tracer
        self.raw_events = RawEventRepository(session, logger, tracer)
        self.customer_profiles = CustomerProfileRepository(session, logger, tracer)
        self.identities = IdentityRepository(session, logger, tracer)
        self.purchases = PurchaseRepository(session, logger, tracer)
        self.segments = SegmentRepository(session, logger, tracer)

    async def commit(self) -> None:
        with self._tracer.start_span('repo.event_processing_transaction.commit'):
            try:
                await self._session.commit()
            except Exception as exc:
                raise UseCaseDependencyError('commit failed') from exc
            finally:
                await self._session.close()

    async def rollback(self) -> None:
        with self._tracer.start_span('repo.event_processing_transaction.rollback'):
            try:
                await self._session.rollback()
            except Exception as exc:
                raise UseCaseDependencyError('rollback failed') from exc
            finally:
                await self._session.close()


class SqlAlchemyEventProcessingUnitOfWork(EventProcessingUnitOfWork):
    def __init__(
        self,
        session_factory: Callable[[], AsyncSession],
        logger: Logger,
        tracer: Tracer,
    ) -> None:
        self._session_factory = session_factory
        self._logger = logger
        self._tracer = tracer

    async def begin(self) -> EventProcessingTransaction:
        session = self._session_factory()
        return SqlAlchemyEventProcessingTransaction(
            session, self._logger, self._tracer
        )
