"""Add missing tables: application_history, notes, attachments, warming, rate_limiting

Revision ID: c3a4b5d6e7f8
Revises: 585794b66272
Create Date: 2026-01-01 10:00:00.000000

This migration adds:
1. application_history - Track all status changes and updates
2. application_notes - Notes and comments on applications
3. application_attachments - File attachments for applications
4. email_warming_configs - Email warming configuration
5. email_warming_daily_logs - Daily warming progress logs
6. rate_limit_configs - Rate limiting configuration
7. rate_limit_usage_logs - Rate limit usage tracking

Also updates:
- applications table with interview fields
- ApplicationStatusEnum with new values
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c3a4b5d6e7f8"
down_revision: Union[str, Sequence[str], None] = "585794b66272"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Add missing tables and columns."""

    # ===========================================
    # 1. UPDATE ApplicationStatusEnum
    # ===========================================
    # Note: For SQLite, we can't easily alter enums. The enum values in the model
    # will work as strings in SQLite. For PostgreSQL, you would need:
    # op.execute("ALTER TYPE applicationstatusenum ADD VALUE 'RESPONDED'")
    # etc.

    # ===========================================
    # 2. ADD INTERVIEW FIELDS TO APPLICATIONS
    # ===========================================
    # Use batch mode for SQLite compatibility
    with op.batch_alter_table("applications") as batch_op:
        batch_op.add_column(
            sa.Column("interview_date", sa.DateTime(timezone=True), nullable=True)
        )
        batch_op.add_column(
            sa.Column("interview_type", sa.String(length=50), nullable=True)
        )
        batch_op.add_column(sa.Column("interview_notes", sa.Text(), nullable=True))

    # ===========================================
    # 3. CREATE APPLICATION_HISTORY TABLE
    # ===========================================
    op.create_table(
        "application_history",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("application_id", sa.Integer(), nullable=False),
        sa.Column("changed_by", sa.Integer(), nullable=True),
        sa.Column("field_name", sa.String(length=100), nullable=True),
        sa.Column("old_value", sa.Text(), nullable=True),
        sa.Column("new_value", sa.Text(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("change_type", sa.String(length=50), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["application_id"], ["applications.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["changed_by"], ["candidates.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_application_history_id"), "application_history", ["id"], unique=False
    )
    op.create_index(
        op.f("ix_application_history_application_id"),
        "application_history",
        ["application_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_application_history_change_type"),
        "application_history",
        ["change_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_application_history_created_at"),
        "application_history",
        ["created_at"],
        unique=False,
    )
    op.create_index(
        "ix_app_history_app_created",
        "application_history",
        ["application_id", "created_at"],
        unique=False,
    )

    # ===========================================
    # 4. CREATE APPLICATION_NOTES TABLE
    # ===========================================
    op.create_table(
        "application_notes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("application_id", sa.Integer(), nullable=False),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("note_type", sa.String(length=50), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["application_id"], ["applications.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["candidate_id"], ["candidates.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_application_notes_id"), "application_notes", ["id"], unique=False
    )
    op.create_index(
        op.f("ix_application_notes_application_id"),
        "application_notes",
        ["application_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_application_notes_candidate_id"),
        "application_notes",
        ["candidate_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_application_notes_note_type"),
        "application_notes",
        ["note_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_application_notes_created_at"),
        "application_notes",
        ["created_at"],
        unique=False,
    )
    op.create_index(
        "ix_app_notes_app_created",
        "application_notes",
        ["application_id", "created_at"],
        unique=False,
    )

    # ===========================================
    # 5. CREATE APPLICATION_ATTACHMENTS TABLE
    # ===========================================
    op.create_table(
        "application_attachments",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("application_id", sa.Integer(), nullable=False),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("file_path", sa.String(length=500), nullable=False),
        sa.Column("file_type", sa.String(length=100), nullable=True),
        sa.Column("file_size", sa.Integer(), nullable=True),
        sa.Column("attachment_type", sa.String(length=50), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "uploaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["application_id"], ["applications.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["candidate_id"], ["candidates.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_application_attachments_id"),
        "application_attachments",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_application_attachments_application_id"),
        "application_attachments",
        ["application_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_application_attachments_candidate_id"),
        "application_attachments",
        ["candidate_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_application_attachments_file_type"),
        "application_attachments",
        ["file_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_application_attachments_attachment_type"),
        "application_attachments",
        ["attachment_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_application_attachments_uploaded_at"),
        "application_attachments",
        ["uploaded_at"],
        unique=False,
    )
    op.create_index(
        "ix_app_attach_app_uploaded",
        "application_attachments",
        ["application_id", "uploaded_at"],
        unique=False,
    )

    # ===========================================
    # 6. CREATE EMAIL_WARMING_CONFIGS TABLE
    # ===========================================
    op.create_table(
        "email_warming_configs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("strategy", sa.String(length=50), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column("custom_schedule", sa.JSON(), nullable=True),
        sa.Column("current_day", sa.Integer(), nullable=False, default=0),
        sa.Column("emails_sent_today", sa.Integer(), nullable=False, default=0),
        sa.Column("total_emails_sent", sa.Integer(), nullable=False, default=0),
        sa.Column("start_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completion_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_reset_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("success_rate", sa.Float(), nullable=True, default=0.0),
        sa.Column("bounce_rate", sa.Float(), nullable=True, default=0.0),
        sa.Column("auto_progress", sa.Boolean(), nullable=True, default=True),
        sa.Column("pause_on_high_bounce", sa.Boolean(), nullable=True, default=True),
        sa.Column("daily_reset_hour", sa.Integer(), nullable=True, default=0),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["candidate_id"], ["candidates.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("candidate_id"),
    )
    op.create_index(
        op.f("ix_email_warming_configs_id"),
        "email_warming_configs",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_email_warming_configs_deleted_at"),
        "email_warming_configs",
        ["deleted_at"],
        unique=False,
    )

    # ===========================================
    # 7. CREATE EMAIL_WARMING_DAILY_LOGS TABLE
    # ===========================================
    op.create_table(
        "email_warming_daily_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("config_id", sa.Integer(), nullable=False),
        sa.Column("day_number", sa.Integer(), nullable=False),
        sa.Column("date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("daily_limit", sa.Integer(), nullable=False),
        sa.Column("emails_sent", sa.Integer(), nullable=True, default=0),
        sa.Column("emails_delivered", sa.Integer(), nullable=True, default=0),
        sa.Column("emails_bounced", sa.Integer(), nullable=True, default=0),
        sa.Column("emails_failed", sa.Integer(), nullable=True, default=0),
        sa.Column("delivery_rate", sa.Float(), nullable=True, default=0.0),
        sa.Column("bounce_rate", sa.Float(), nullable=True, default=0.0),
        sa.Column("limit_reached", sa.Boolean(), nullable=True, default=False),
        sa.Column("notes", sa.String(length=500), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["config_id"], ["email_warming_configs.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_email_warming_daily_logs_id"),
        "email_warming_daily_logs",
        ["id"],
        unique=False,
    )

    # ===========================================
    # 8. CREATE RATE_LIMIT_CONFIGS TABLE
    # ===========================================
    op.create_table(
        "rate_limit_configs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("preset", sa.String(length=50), nullable=False),
        sa.Column("daily_limit", sa.Integer(), nullable=False, default=100),
        sa.Column("hourly_limit", sa.Integer(), nullable=False, default=25),
        sa.Column("weekly_limit", sa.Integer(), nullable=True),
        sa.Column("monthly_limit", sa.Integer(), nullable=True),
        sa.Column("emails_sent_today", sa.Integer(), nullable=False, default=0),
        sa.Column("emails_sent_this_hour", sa.Integer(), nullable=False, default=0),
        sa.Column("emails_sent_this_week", sa.Integer(), nullable=False, default=0),
        sa.Column("emails_sent_this_month", sa.Integer(), nullable=False, default=0),
        sa.Column("last_hourly_reset", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_daily_reset", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_weekly_reset", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_monthly_reset", sa.DateTime(timezone=True), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, default=True),
        sa.Column("pause_on_limit", sa.Boolean(), nullable=True, default=True),
        sa.Column("notify_on_limit", sa.Boolean(), nullable=True, default=True),
        sa.Column("auto_reset", sa.Boolean(), nullable=True, default=True),
        sa.Column("warning_threshold_daily", sa.Integer(), nullable=True, default=80),
        sa.Column("warning_threshold_hourly", sa.Integer(), nullable=True, default=80),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["candidate_id"], ["candidates.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("candidate_id"),
    )
    op.create_index(
        op.f("ix_rate_limit_configs_id"), "rate_limit_configs", ["id"], unique=False
    )
    op.create_index(
        op.f("ix_rate_limit_configs_deleted_at"),
        "rate_limit_configs",
        ["deleted_at"],
        unique=False,
    )

    # ===========================================
    # 9. CREATE RATE_LIMIT_USAGE_LOGS TABLE
    # ===========================================
    op.create_table(
        "rate_limit_usage_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("config_id", sa.Integer(), nullable=False),
        sa.Column("period_type", sa.String(length=20), nullable=False),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("limit_value", sa.Integer(), nullable=False),
        sa.Column("emails_sent", sa.Integer(), nullable=True, default=0),
        sa.Column("limit_reached", sa.Boolean(), nullable=True, default=False),
        sa.Column("limit_exceeded", sa.Boolean(), nullable=True, default=False),
        sa.Column("usage_percentage", sa.Integer(), nullable=True, default=0),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["config_id"], ["rate_limit_configs.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_rate_limit_usage_logs_id"),
        "rate_limit_usage_logs",
        ["id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema - Remove added tables and columns."""

    # Drop tables in reverse order (due to foreign key constraints)
    op.drop_index(
        op.f("ix_rate_limit_usage_logs_id"), table_name="rate_limit_usage_logs"
    )
    op.drop_table("rate_limit_usage_logs")

    op.drop_index(
        op.f("ix_rate_limit_configs_deleted_at"), table_name="rate_limit_configs"
    )
    op.drop_index(op.f("ix_rate_limit_configs_id"), table_name="rate_limit_configs")
    op.drop_table("rate_limit_configs")

    op.drop_index(
        op.f("ix_email_warming_daily_logs_id"), table_name="email_warming_daily_logs"
    )
    op.drop_table("email_warming_daily_logs")

    op.drop_index(
        op.f("ix_email_warming_configs_deleted_at"), table_name="email_warming_configs"
    )
    op.drop_index(
        op.f("ix_email_warming_configs_id"), table_name="email_warming_configs"
    )
    op.drop_table("email_warming_configs")

    op.drop_index("ix_app_attach_app_uploaded", table_name="application_attachments")
    op.drop_index(
        op.f("ix_application_attachments_uploaded_at"),
        table_name="application_attachments",
    )
    op.drop_index(
        op.f("ix_application_attachments_attachment_type"),
        table_name="application_attachments",
    )
    op.drop_index(
        op.f("ix_application_attachments_file_type"),
        table_name="application_attachments",
    )
    op.drop_index(
        op.f("ix_application_attachments_candidate_id"),
        table_name="application_attachments",
    )
    op.drop_index(
        op.f("ix_application_attachments_application_id"),
        table_name="application_attachments",
    )
    op.drop_index(
        op.f("ix_application_attachments_id"), table_name="application_attachments"
    )
    op.drop_table("application_attachments")

    op.drop_index("ix_app_notes_app_created", table_name="application_notes")
    op.drop_index(
        op.f("ix_application_notes_created_at"), table_name="application_notes"
    )
    op.drop_index(
        op.f("ix_application_notes_note_type"), table_name="application_notes"
    )
    op.drop_index(
        op.f("ix_application_notes_candidate_id"), table_name="application_notes"
    )
    op.drop_index(
        op.f("ix_application_notes_application_id"), table_name="application_notes"
    )
    op.drop_index(op.f("ix_application_notes_id"), table_name="application_notes")
    op.drop_table("application_notes")

    op.drop_index("ix_app_history_app_created", table_name="application_history")
    op.drop_index(
        op.f("ix_application_history_created_at"), table_name="application_history"
    )
    op.drop_index(
        op.f("ix_application_history_change_type"), table_name="application_history"
    )
    op.drop_index(
        op.f("ix_application_history_application_id"), table_name="application_history"
    )
    op.drop_index(op.f("ix_application_history_id"), table_name="application_history")
    op.drop_table("application_history")

    # Remove interview columns from applications
    # Use batch mode for SQLite compatibility
    with op.batch_alter_table("applications") as batch_op:
        batch_op.drop_column("interview_notes")
        batch_op.drop_column("interview_type")
        batch_op.drop_column("interview_date")
