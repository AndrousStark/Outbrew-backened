"""add template versioning

Revision ID: 37725b57f1c8
Revises: d191b2c86fc0
Create Date: 2026-01-12 15:15:26.291146

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '37725b57f1c8'
down_revision: Union[str, Sequence[str], None] = 'd191b2c86fc0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Add template versioning table."""

    # Create template_versions table
    op.create_table(
        'template_versions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('template_id', sa.Integer(), nullable=False),
        sa.Column('version_number', sa.Integer(), nullable=False),
        sa.Column('version_name', sa.String(length=100)),
        sa.Column('change_description', sa.Text()),
        sa.Column('subject_template', sa.String(length=500), nullable=False),
        sa.Column('body_template_text', sa.Text(), nullable=False),
        sa.Column('body_template_html', sa.Text()),
        sa.Column('tags', sa.JSON(), server_default='[]'),
        sa.Column('variables', sa.JSON(), server_default='[]'),
        sa.Column('changed_by_id', sa.Integer()),
        sa.Column('changed_by_name', sa.String(length=255)),
        sa.Column('total_uses_at_version', sa.Integer(), server_default='0'),
        sa.Column('avg_rating_at_version', sa.Float(), server_default='0.0'),
        sa.Column('is_current', sa.Boolean(), server_default='false'),
        sa.Column('is_published', sa.Boolean(), server_default='true'),
        sa.Column('changes_summary', sa.JSON(), server_default='{}'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['template_id'], ['public_templates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['changed_by_id'], ['candidates.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_template_version_template_id', 'template_versions', ['template_id'])
    op.create_index('ix_template_version_template_version', 'template_versions', ['template_id', 'version_number'], unique=True)
    op.create_index('ix_template_version_current', 'template_versions', ['template_id', 'is_current'])


def downgrade() -> None:
    """Downgrade schema - Remove template versioning table."""
    op.drop_index('ix_template_version_current', table_name='template_versions')
    op.drop_index('ix_template_version_template_version', table_name='template_versions')
    op.drop_index('ix_template_version_template_id', table_name='template_versions')
    op.drop_table('template_versions')
