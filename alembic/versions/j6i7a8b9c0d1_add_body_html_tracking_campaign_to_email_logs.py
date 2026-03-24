"""Add body_html, tracking_id, campaign_id to email_logs

Revision ID: j6i7a8b9c0d1
Revises: i5h6a7b8c9d0
Create Date: 2026-02-25 10:00:00.000000

Adds missing columns to email_logs table:
1. body_html - stores rendered HTML body
2. tracking_id - unique tracking identifier for open/click tracking
3. campaign_id - foreign key to group_campaigns
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'j6i7a8b9c0d1'
down_revision: Union[str, None] = 'i5h6a7b8c9d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add body_html column
    op.add_column('email_logs', sa.Column('body_html', sa.Text(), nullable=True))

    # Add tracking_id column with unique constraint and index
    op.add_column('email_logs', sa.Column('tracking_id', sa.String(100), nullable=True))
    op.create_index('ix_email_logs_tracking_id', 'email_logs', ['tracking_id'], unique=True)

    # Add campaign_id column with foreign key and index
    op.add_column('email_logs', sa.Column('campaign_id', sa.Integer(), nullable=True))
    op.create_index('ix_email_logs_campaign_id', 'email_logs', ['campaign_id'])
    # Note: SQLite doesn't enforce FK constraints by default, but adding for schema correctness
    with op.batch_alter_table('email_logs') as batch_op:
        batch_op.create_foreign_key(
            'fk_email_logs_campaign_id',
            'group_campaigns',
            ['campaign_id'],
            ['id'],
            ondelete='SET NULL'
        )


def downgrade() -> None:
    with op.batch_alter_table('email_logs') as batch_op:
        batch_op.drop_constraint('fk_email_logs_campaign_id', type_='foreignkey')
    op.drop_index('ix_email_logs_campaign_id', table_name='email_logs')
    op.drop_index('ix_email_logs_tracking_id', table_name='email_logs')
    op.drop_column('email_logs', 'campaign_id')
    op.drop_column('email_logs', 'tracking_id')
    op.drop_column('email_logs', 'body_html')
