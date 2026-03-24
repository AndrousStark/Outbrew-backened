"""add enrichment job tracking table

Revision ID: f2e3a4b5c6d7
Revises: 37725b57f1c8
Create Date: 2026-01-13 10:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f2e3a4b5c6d7"
down_revision: Union[str, Sequence[str], None] = "37725b57f1c8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Add enrichment_jobs table for job tracking and retry logic."""

    # Create enrichment_jobs table
    op.create_table(
        "enrichment_jobs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.String(length=36), nullable=False, unique=True),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column(
            "status", sa.String(length=20), nullable=False, server_default="pending"
        ),  # pending, running, completed, failed, paused
        sa.Column("current_step", sa.String(length=255)),
        sa.Column("completed_recipients", sa.Integer(), server_default="0"),
        sa.Column("failed_recipients", sa.Integer(), server_default="0"),
        sa.Column("total_recipients", sa.Integer(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), server_default="1"),
        sa.Column("max_attempts", sa.Integer(), server_default="3"),
        sa.Column("last_error", sa.Text()),
        sa.Column("enrichment_config", sa.JSON(), server_default="{}"),
        sa.Column("enrichment_results", sa.JSON(), server_default="{}"),
        sa.Column("validation_results", sa.JSON(), server_default="{}"),
        sa.Column("statistics", sa.JSON(), server_default="{}"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("next_retry_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(
            ["candidate_id"], ["candidates.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create indexes for efficient queries
    op.create_index("ix_enrichment_job_id", "enrichment_jobs", ["job_id"], unique=True)
    op.create_index(
        "ix_enrichment_job_candidate_id", "enrichment_jobs", ["candidate_id"]
    )
    op.create_index("ix_enrichment_job_status", "enrichment_jobs", ["status"])
    op.create_index("ix_enrichment_job_created_at", "enrichment_jobs", ["created_at"])
    op.create_index(
        "ix_enrichment_job_next_retry_at", "enrichment_jobs", ["next_retry_at"]
    )


def downgrade() -> None:
    """Downgrade schema - Remove enrichment_jobs table."""
    op.drop_index("ix_enrichment_job_next_retry_at", table_name="enrichment_jobs")
    op.drop_index("ix_enrichment_job_created_at", table_name="enrichment_jobs")
    op.drop_index("ix_enrichment_job_status", table_name="enrichment_jobs")
    op.drop_index("ix_enrichment_job_candidate_id", table_name="enrichment_jobs")
    op.drop_index("ix_enrichment_job_id", table_name="enrichment_jobs")
    op.drop_table("enrichment_jobs")
