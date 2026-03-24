"""add merge history tracking table

Revision ID: g3f4a5b6c7d8
Revises: f2e3a4b5c6d7
Create Date: 2026-01-13 11:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "g3f4a5b6c7d8"
down_revision: Union[str, Sequence[str], None] = "f2e3a4b5c6d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Add merge_history table for deduplication audit and rollback."""

    # Create merge_history table
    op.create_table(
        "merge_history",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("merge_id", sa.String(length=36), nullable=False, unique=True),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("primary_recipient_id", sa.Integer(), nullable=False),
        sa.Column("secondary_recipient_ids", sa.JSON(), nullable=False),
        sa.Column(
            "strategy", sa.String(length=50), nullable=False
        ),  # keep_first, keep_most_complete, keep_most_recent
        sa.Column("confidence_score", sa.Integer(), server_default="0"),
        sa.Column("merged_fields", sa.JSON(), nullable=False),
        sa.Column("conflicts_resolved", sa.JSON(), nullable=False),
        sa.Column("data_snapshot", sa.JSON()),
        sa.Column(
            "status", sa.String(length=20), server_default="completed"
        ),  # completed, rolled_back
        sa.Column("rolled_back_at", sa.DateTime(timezone=True)),
        sa.Column("rollback_reason", sa.Text()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column("created_by", sa.String(length=255)),
        sa.Column("notes", sa.Text()),
        sa.ForeignKeyConstraint(
            ["candidate_id"], ["candidates.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create indexes for efficient queries
    op.create_index(
        "ix_merge_history_merge_id", "merge_history", ["merge_id"], unique=True
    )
    op.create_index("ix_merge_history_candidate_id", "merge_history", ["candidate_id"])
    op.create_index("ix_merge_history_status", "merge_history", ["status"])
    op.create_index("ix_merge_history_created_at", "merge_history", ["created_at"])
    op.create_index(
        "ix_merge_history_primary_recipient_id",
        "merge_history",
        ["primary_recipient_id"],
    )


def downgrade() -> None:
    """Downgrade schema - Remove merge_history table."""
    op.drop_index("ix_merge_history_primary_recipient_id", table_name="merge_history")
    op.drop_index("ix_merge_history_created_at", table_name="merge_history")
    op.drop_index("ix_merge_history_status", table_name="merge_history")
    op.drop_index("ix_merge_history_candidate_id", table_name="merge_history")
    op.drop_index("ix_merge_history_merge_id", table_name="merge_history")
    op.drop_table("merge_history")
