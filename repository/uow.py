from collections.abc import Callable

from sqlalchemy.ext.asyncio import AsyncSession

from repository.activation import ActivationDeliveryRepository, ActivationJobRepository
from usecase.error import UseCaseDependencyError
from usecase.interface import ExportTransaction, ExportUnitOfWork


class SqlAlchemyExportTransaction(ExportTransaction):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self.jobs = ActivationJobRepository(session)
        self.deliveries = ActivationDeliveryRepository(session)

    async def commit(self) -> None:
        try:
            await self._session.commit()
        except Exception as exc:
            raise UseCaseDependencyError('commit failed') from exc
        finally:
            await self._session.close()

    async def rollback(self) -> None:
        try:
            await self._session.rollback()
        except Exception as exc:
            raise UseCaseDependencyError('rollback failed') from exc
        finally:
            await self._session.close()


class SqlAlchemyExportUnitOfWork(ExportUnitOfWork):
    def __init__(self, session_factory: Callable[[], AsyncSession]) -> None:
        self._session_factory = session_factory

    async def begin(self) -> ExportTransaction:
        session = self._session_factory()
        return SqlAlchemyExportTransaction(session)
