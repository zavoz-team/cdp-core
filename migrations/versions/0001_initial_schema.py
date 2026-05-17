"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-05-17

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '0001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'raw_events',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('event_id', sa.String(), nullable=False),
        sa.Column('event_type', sa.String(), nullable=False),
        sa.Column('source', sa.String(), nullable=False),
        sa.Column('occurred_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('received_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('identifiers_json', postgresql.JSONB(), nullable=False),
        sa.Column('attributes_json', postgresql.JSONB(), nullable=True),
        sa.Column('payload_json', postgresql.JSONB(), nullable=False),
        sa.Column('trace_context_json', postgresql.JSONB(), nullable=True),
        sa.Column('processing_status', sa.String(), nullable=False),
        sa.Column('error_reason', sa.String(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id', name='pk_raw_events'),
        sa.UniqueConstraint('event_id', name='uq_raw_events_event_id'),
        sa.CheckConstraint(
            "processing_status IN ('received', 'processed', 'ignored_anonymous', 'duplicate', 'failed', 'sent_to_dlq')",
            name='ck_raw_events_processing_status',
        ),
    )
    op.create_index('ix_raw_events_processing_status', 'raw_events', ['processing_status'])

    op.create_table(
        'customer_profiles',
        sa.Column('customer_id', sa.String(), nullable=False),
        sa.Column('attributes_json', postgresql.JSONB(), nullable=True),
        sa.Column('first_seen_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('last_seen_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('last_purchase_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('events_count', sa.BigInteger(), server_default=sa.text('0'), nullable=False),
        sa.Column('page_views_count', sa.BigInteger(), server_default=sa.text('0'), nullable=False),
        sa.Column('cart_adds_count', sa.BigInteger(), server_default=sa.text('0'), nullable=False),
        sa.Column('orders_count', sa.BigInteger(), server_default=sa.text('0'), nullable=False),
        sa.Column('total_revenue', sa.Numeric(18, 2), server_default=sa.text('0'), nullable=False),
        sa.Column('currency', sa.String(), server_default=sa.text("'RUB'"), nullable=False),
        sa.Column('recent_events_json', postgresql.JSONB(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('customer_id', name='pk_customer_profiles'),
        sa.CheckConstraint("currency = 'RUB'", name='ck_customer_profiles_currency'),
        sa.CheckConstraint('events_count >= 0', name='ck_customer_profiles_events_count'),
        sa.CheckConstraint('page_views_count >= 0', name='ck_customer_profiles_page_views_count'),
        sa.CheckConstraint('cart_adds_count >= 0', name='ck_customer_profiles_cart_adds_count'),
        sa.CheckConstraint('orders_count >= 0', name='ck_customer_profiles_orders_count'),
        sa.CheckConstraint('total_revenue >= 0', name='ck_customer_profiles_total_revenue'),
    )

    op.create_table(
        'identity_links',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('identity_type', sa.String(), nullable=False),
        sa.Column('identity_value', sa.String(), nullable=False),
        sa.Column('customer_id', sa.String(), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id', name='pk_identity_links'),
        sa.ForeignKeyConstraint(
            ['customer_id'],
            ['customer_profiles.customer_id'],
            name='fk_identity_links_customer_id',
        ),
        sa.UniqueConstraint('identity_type', 'identity_value', name='uq_identity_links_type_value'),
        sa.CheckConstraint(
            "identity_type IN ('email', 'phone', 'external_user_id')",
            name='ck_identity_links_identity_type',
        ),
    )
    op.create_index('ix_identity_links_customer_id', 'identity_links', ['customer_id'])

    op.create_table(
        'processed_purchases',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('source', sa.String(), nullable=False),
        sa.Column('order_id', sa.String(), nullable=False),
        sa.Column('customer_id', sa.String(), nullable=False),
        sa.Column('event_id', sa.String(), nullable=False),
        sa.Column('amount', sa.Numeric(18, 2), nullable=False),
        sa.Column('currency', sa.String(), server_default=sa.text("'RUB'"), nullable=False),
        sa.Column('occurred_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id', name='pk_processed_purchases'),
        sa.ForeignKeyConstraint(
            ['customer_id'],
            ['customer_profiles.customer_id'],
            name='fk_processed_purchases_customer_id',
        ),
        sa.UniqueConstraint('source', 'order_id', name='uq_processed_purchases_source_order_id'),
        sa.CheckConstraint("currency = 'RUB'", name='ck_processed_purchases_currency'),
        sa.CheckConstraint('amount >= 0', name='ck_processed_purchases_amount'),
    )
    op.create_index('ix_processed_purchases_customer_id', 'processed_purchases', ['customer_id'])

    op.create_table(
        'segment_definitions',
        sa.Column('segment_id', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), server_default=sa.text('true'), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('segment_id', name='pk_segment_definitions'),
        sa.CheckConstraint(
            "segment_id IN ('new_user', 'active', 'vip')",
            name='ck_segment_definitions_segment_id',
        ),
    )

    op.create_table(
        'segment_memberships',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('segment_id', sa.String(), nullable=False),
        sa.Column('customer_id', sa.String(), nullable=False),
        sa.Column('matched_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id', name='pk_segment_memberships'),
        sa.ForeignKeyConstraint(
            ['segment_id'],
            ['segment_definitions.segment_id'],
            name='fk_segment_memberships_segment_id',
        ),
        sa.ForeignKeyConstraint(
            ['customer_id'],
            ['customer_profiles.customer_id'],
            name='fk_segment_memberships_customer_id',
        ),
        sa.UniqueConstraint('segment_id', 'customer_id', name='uq_segment_memberships_segment_customer'),
    )
    op.create_index('ix_segment_memberships_customer_id', 'segment_memberships', ['customer_id'])

    op.create_table(
        'activation_jobs',
        sa.Column('job_id', sa.String(), nullable=False),
        sa.Column('segment_id', sa.String(), nullable=False),
        sa.Column('destination_type', sa.String(), server_default=sa.text("'webhook'"), nullable=False),
        sa.Column('destination_url', sa.String(), nullable=False),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('members_count', sa.BigInteger(), server_default=sa.text('0'), nullable=False),
        sa.Column('requested_by_user_id', sa.String(), nullable=True),
        sa.Column('requested_by_email', sa.String(), nullable=True),
        sa.Column('requested_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('started_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('completed_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('error_reason', sa.String(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('job_id', name='pk_activation_jobs'),
        sa.ForeignKeyConstraint(
            ['segment_id'],
            ['segment_definitions.segment_id'],
            name='fk_activation_jobs_segment_id',
        ),
        sa.CheckConstraint("destination_type = 'webhook'", name='ck_activation_jobs_destination_type'),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'failed', 'partially_failed')",
            name='ck_activation_jobs_status',
        ),
        sa.CheckConstraint('members_count >= 0', name='ck_activation_jobs_members_count'),
    )
    op.create_index('ix_activation_jobs_segment_id', 'activation_jobs', ['segment_id'])
    op.create_index('ix_activation_jobs_status', 'activation_jobs', ['status'])

    op.create_table(
        'activation_deliveries',
        sa.Column('delivery_id', sa.String(), nullable=False),
        sa.Column('job_id', sa.String(), nullable=False),
        sa.Column('attempt_number', sa.Integer(), server_default=sa.text('1'), nullable=False),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('request_payload_json', postgresql.JSONB(), nullable=True),
        sa.Column('response_status_code', sa.Integer(), nullable=True),
        sa.Column('error_reason', sa.String(), nullable=True),
        sa.Column('started_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('completed_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('delivery_id', name='pk_activation_deliveries'),
        sa.ForeignKeyConstraint(
            ['job_id'],
            ['activation_jobs.job_id'],
            name='fk_activation_deliveries_job_id',
        ),
        sa.UniqueConstraint('job_id', 'attempt_number', name='uq_activation_deliveries_job_attempt'),
        sa.CheckConstraint('attempt_number = 1', name='ck_activation_deliveries_attempt_number'),
        sa.CheckConstraint(
            "status IN ('succeeded', 'failed')",
            name='ck_activation_deliveries_status',
        ),
    )


def downgrade() -> None:
    op.drop_table('activation_deliveries')
    op.drop_table('activation_jobs')
    op.drop_table('segment_memberships')
    op.drop_table('segment_definitions')
    op.drop_table('processed_purchases')
    op.drop_table('identity_links')
    op.drop_table('customer_profiles')
    op.drop_table('raw_events')
