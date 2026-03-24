"""add email labels and filters tables

Revision ID: d191b2c86fc0
Revises: 4c416b4505e2
Create Date: 2026-01-12 15:10:26.905487

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd191b2c86fc0'
down_revision: Union[str, Sequence[str], None] = '4c416b4505e2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Add email labels and filters tables."""

    # Create email_labels table
    op.create_table(
        'email_labels',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('color', sa.String(length=7), server_default='#808080'),
        sa.Column('description', sa.String(length=255)),
        sa.Column('email_count', sa.Integer(), server_default='0'),
        sa.Column('is_system', sa.Boolean(), server_default='false'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), onupdate=sa.func.now()),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_email_labels_candidate', 'email_labels', ['candidate_id'])
    op.create_index('idx_email_labels_name', 'email_labels', ['candidate_id', 'name'], unique=True)

    # Create email_label_assignments table
    op.create_table(
        'email_label_assignments',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('message_id', sa.Integer(), nullable=False),
        sa.Column('label_id', sa.Integer(), nullable=False),
        sa.Column('assigned_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['message_id'], ['email_messages.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['label_id'], ['email_labels.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_label_assignments_message', 'email_label_assignments', ['message_id'])
    op.create_index('idx_label_assignments_label', 'email_label_assignments', ['label_id'])
    op.create_index('idx_label_assignments_unique', 'email_label_assignments', ['message_id', 'label_id'], unique=True)

    # Create email_filters table
    op.create_table(
        'email_filters',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('description', sa.String(length=255)),
        sa.Column('is_enabled', sa.Boolean(), server_default='true'),
        sa.Column('conditions', sa.Text(), nullable=False),
        sa.Column('match_all', sa.Boolean(), server_default='true'),
        sa.Column('actions', sa.Text(), nullable=False),
        sa.Column('times_matched', sa.Integer(), server_default='0'),
        sa.Column('last_matched_at', sa.DateTime(timezone=True)),
        sa.Column('priority', sa.Integer(), server_default='100'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), onupdate=sa.func.now()),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_email_filters_candidate', 'email_filters', ['candidate_id'])
    op.create_index('idx_email_filters_enabled', 'email_filters', ['candidate_id', 'is_enabled'])
    op.create_index('idx_email_filters_priority', 'email_filters', ['candidate_id', 'priority'])


def downgrade() -> None:
    """Downgrade schema - Remove email labels and filters tables."""
    op.drop_index('idx_email_filters_priority', table_name='email_filters')
    op.drop_index('idx_email_filters_enabled', table_name='email_filters')
    op.drop_index('idx_email_filters_candidate', table_name='email_filters')
    op.drop_table('email_filters')

    op.drop_index('idx_label_assignments_unique', table_name='email_label_assignments')
    op.drop_index('idx_label_assignments_label', table_name='email_label_assignments')
    op.drop_index('idx_label_assignments_message', table_name='email_label_assignments')
    op.drop_table('email_label_assignments')

    op.drop_index('idx_email_labels_name', table_name='email_labels')
    op.drop_index('idx_email_labels_candidate', table_name='email_labels')
    op.drop_table('email_labels')
