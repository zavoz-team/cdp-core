from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from domain.identity import IdentityLink, KnownIdentifier, KnownIdentifierType
from usecase.error import UseCaseDependencyError

_SELECT_SQL = """
    SELECT identity_type, identity_value, customer_id
    FROM identity_links
    WHERE identity_type = :identity_type AND identity_value = :identity_value
"""

_INSERT_SQL = """
    INSERT INTO identity_links (identity_type, identity_value, customer_id, created_at)
    VALUES (:identity_type, :identity_value, :customer_id, :created_at)
    ON CONFLICT (identity_type, identity_value) DO NOTHING
"""


class IdentityRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_link(self, identifier: KnownIdentifier) -> IdentityLink | None:
        try:
            result = await self._session.execute(
                sa.text(_SELECT_SQL),
                {
                    'identity_type': identifier.identifier_type.value,
                    'identity_value': identifier.value,
                },
            )
        except Exception as exc:
            raise UseCaseDependencyError('identity link lookup failed') from exc

        row = result.mappings().first()
        if row is None:
            return None
        return _row_to_link(row)

    async def find_links(
        self,
        identifiers: tuple[KnownIdentifier, ...],
    ) -> tuple[IdentityLink, ...]:
        if not identifiers:
            return ()

        conditions = ' OR '.join(
            f'(identity_type = :type_{i} AND identity_value = :val_{i})'
            for i in range(len(identifiers))
        )
        sql = f"""
            SELECT identity_type, identity_value, customer_id
            FROM identity_links
            WHERE {conditions}
        """
        params: dict[str, Any] = {}
        for i, identifier in enumerate(identifiers):
            params[f'type_{i}'] = identifier.identifier_type.value
            params[f'val_{i}'] = identifier.value

        try:
            result = await self._session.execute(sa.text(sql), params)
        except Exception as exc:
            raise UseCaseDependencyError('identity links find failed') from exc

        return tuple(_row_to_link(row) for row in result.mappings().all())

    async def save_links(self, links: tuple[IdentityLink, ...]) -> None:
        if not links:
            return

        now = datetime.now(timezone.utc)
        try:
            await self._session.execute(
                sa.text(_INSERT_SQL),
                [
                    {
                        'identity_type': link.identity_type.value,
                        'identity_value': link.identity_value,
                        'customer_id': link.customer_id,
                        'created_at': now,
                    }
                    for link in links
                ],
            )
        except Exception as exc:
            raise UseCaseDependencyError('identity links save failed') from exc


def _row_to_link(row: Any) -> IdentityLink:
    return IdentityLink(
        identity_type=KnownIdentifierType(row['identity_type']),
        identity_value=row['identity_value'],
        customer_id=row['customer_id'],
    )
