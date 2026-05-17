from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from domain.profile import Currency
from usecase.dto import ProcessedPurchase, PurchaseRecordOutcome, PurchaseRecordResult
from usecase.error import UseCaseDependencyError

_SELECT_SQL = """
    SELECT source, order_id, customer_id, event_id, amount, currency, occurred_at
    FROM processed_purchases
    WHERE source = :source AND order_id = :order_id
"""

_INSERT_SQL = """
    INSERT INTO processed_purchases (
        source, order_id, customer_id, event_id,
        amount, currency, occurred_at, created_at
    ) VALUES (
        :source, :order_id, :customer_id, :event_id,
        :amount, :currency, :occurred_at, :created_at
    )
"""


class PurchaseRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_source_order_id(
        self,
        source: str,
        order_id: str,
    ) -> ProcessedPurchase | None:
        try:
            result = await self._session.execute(
                sa.text(_SELECT_SQL),
                {'source': source, 'order_id': order_id},
            )
        except Exception as exc:
            raise UseCaseDependencyError('purchase lookup failed') from exc

        row = result.mappings().first()
        if row is None:
            return None
        return _row_to_purchase(row)

    async def record_processed_purchase(
        self,
        purchase: ProcessedPurchase,
    ) -> PurchaseRecordResult:
        existing = await self.get_by_source_order_id(purchase.source, purchase.order_id)

        if existing is not None:
            outcome = (
                PurchaseRecordOutcome.DUPLICATE
                if _is_duplicate(existing, purchase)
                else PurchaseRecordOutcome.CONFLICT
            )
            return PurchaseRecordResult(
                outcome=outcome,
                source=purchase.source,
                order_id=purchase.order_id,
                existing_purchase=existing,
            )

        try:
            await self._session.execute(
                sa.text(_INSERT_SQL),
                _purchase_to_params(purchase),
            )
        except Exception as exc:
            raise UseCaseDependencyError('purchase record failed') from exc

        return PurchaseRecordResult(
            outcome=PurchaseRecordOutcome.RECORDED,
            source=purchase.source,
            order_id=purchase.order_id,
        )


def _is_duplicate(existing: ProcessedPurchase, incoming: ProcessedPurchase) -> bool:
    return (
        existing.amount == incoming.amount
        and existing.customer_id == incoming.customer_id
        and existing.currency == incoming.currency
    )


def _purchase_to_params(purchase: ProcessedPurchase) -> dict[str, Any]:
    return {
        'source': purchase.source,
        'order_id': purchase.order_id,
        'customer_id': purchase.customer_id,
        'event_id': purchase.event_id,
        'amount': str(purchase.amount),
        'currency': purchase.currency.value,
        'occurred_at': purchase.occurred_at,
        'created_at': datetime.now(timezone.utc),
    }


def _row_to_purchase(row: Any) -> ProcessedPurchase:
    return ProcessedPurchase(
        source=row['source'],
        order_id=row['order_id'],
        customer_id=row['customer_id'],
        event_id=row['event_id'],
        amount=Decimal(str(row['amount'])),
        currency=Currency(row['currency']),
        occurred_at=row['occurred_at'],
    )
