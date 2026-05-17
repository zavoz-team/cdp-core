"""seed static segment definitions

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-17

"""
import datetime
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = '0002'
down_revision: Union[str, None] = '0001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_SEGMENTS = [
    {
        'segment_id': 'new_user',
        'name': 'New User',
        'description': 'Customers who recently registered or placed their first order',
        'is_active': True,
    },
    {
        'segment_id': 'active',
        'name': 'Active',
        'description': 'Customers with recent purchase or engagement activity',
        'is_active': True,
    },
    {
        'segment_id': 'vip',
        'name': 'VIP',
        'description': 'High-value customers based on total revenue',
        'is_active': True,
    },
]


def upgrade() -> None:
    segments_table = sa.table(
        'segment_definitions',
        sa.column('segment_id', sa.String()),
        sa.column('name', sa.String()),
        sa.column('description', sa.Text()),
        sa.column('is_active', sa.Boolean()),
        sa.column('created_at', sa.TIMESTAMP(timezone=True)),
        sa.column('updated_at', sa.TIMESTAMP(timezone=True)),
    )
    op.bulk_insert(
        segments_table,
        [
            {
                'segment_id': s['segment_id'],
                'name': s['name'],
                'description': s['description'],
                'is_active': s['is_active'],
                'created_at': datetime.datetime.now(datetime.timezone.utc),
                'updated_at': datetime.datetime.now(datetime.timezone.utc),
            }
            for s in _SEGMENTS
        ],
    )


def downgrade() -> None:
    op.execute(
        sa.text("DELETE FROM segment_definitions WHERE segment_id IN ('new_user', 'active', 'vip')")
    )
