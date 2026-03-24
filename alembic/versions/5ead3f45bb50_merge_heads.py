"""merge_heads

Revision ID: 5ead3f45bb50
Revises: 467d1a296ab7, j6i7a8b9c0d1
Create Date: 2026-03-11 01:24:54.840120

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5ead3f45bb50'
down_revision: Union[str, Sequence[str], None] = ('467d1a296ab7', 'j6i7a8b9c0d1')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
