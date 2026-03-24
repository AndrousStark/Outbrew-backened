"""Connect recipients to applications

Revision ID: 7b8c9d0e1f2a
Revises: 6a4d80cf472b
Create Date: 2026-01-16 12:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7b8c9d0e1f2a"
down_revision = "6a4d80cf472b"
branch_labels = None
depends_on = None


def upgrade():
    """
    Add recipient_id to applications table to link every email sent
    from Recipients page (ULTRA AI) to an Application record.

    This enables:
    - Tracking ULTRA AI emails as job applications
    - Unified view of all outreach in Pipeline
    - Connecting recipient engagement to application status
    """
    # Use batch mode for SQLite compatibility
    with op.batch_alter_table("applications") as batch_op:
        # Add recipient_id column to applications
        batch_op.add_column(sa.Column("recipient_id", sa.Integer(), nullable=True))

        # Add foreign key constraint
        batch_op.create_foreign_key(
            "fk_application_recipient_id",
            "recipients",
            ["recipient_id"],
            ["id"],
            ondelete="SET NULL",
        )

        # Add index for faster queries
        batch_op.create_index("ix_applications_recipient_id", ["recipient_id"])

        # Add composite index for candidate + recipient queries
        batch_op.create_index(
            "ix_app_candidate_recipient", ["candidate_id", "recipient_id"]
        )


def downgrade():
    """Remove recipient connection from applications"""
    # Use batch mode for SQLite compatibility
    with op.batch_alter_table("applications") as batch_op:
        # Drop indexes
        batch_op.drop_index("ix_app_candidate_recipient")
        batch_op.drop_index("ix_applications_recipient_id")

        # Drop foreign key
        batch_op.drop_constraint("fk_application_recipient_id", type_="foreignkey")

        # Drop column
        batch_op.drop_column("recipient_id")
