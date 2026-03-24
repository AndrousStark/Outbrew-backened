"""add_bounce_count_and_last_contacted_to_recipients

Revision ID: 6a4d80cf472b
Revises: 6598d87d92e2
Create Date: 2026-01-15 23:43:17.011787

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6a4d80cf472b'
down_revision: Union[str, Sequence[str], None] = '6598d87d92e2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add bounce_count and last_contacted_at columns to recipients table."""
    # Add bounce_count column (Integer with default 0)
    op.add_column('recipients', sa.Column('bounce_count', sa.Integer(), nullable=True, server_default='0'))

    # Add last_contacted_at column (DateTime, nullable)
    op.add_column('recipients', sa.Column('last_contacted_at', sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    """Remove bounce_count and last_contacted_at columns from recipients table."""
    op.drop_column('recipients', 'last_contacted_at')
    op.drop_column('recipients', 'bounce_count')
