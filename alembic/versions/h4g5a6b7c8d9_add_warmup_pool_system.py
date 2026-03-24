"""add warmup pool system - ULTRA PREMIUM EDITION

Revision ID: h4g5a6b7c8d9
Revises: g3f4a5b6c7d8
Create Date: 2026-02-02 12:00:00.000000

Creates the complete Email Warmup Pool System with:
- Pool membership and tier management
- Warmup conversations and messages
- Inbox placement testing
- Blacklist monitoring
- Spam rescue logging
- Schedule configuration
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "h4g5a6b7c8d9"
down_revision: Union[str, Sequence[str], None] = "g3f4a5b6c7d8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Add warmup pool system tables."""

    # =========================================================================
    # warmup_pool_members - Core membership table
    # =========================================================================
    op.create_table(
        "warmup_pool_members",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("email_domain", sa.String(length=100), nullable=True),
        sa.Column("email_provider", sa.String(length=50), nullable=True),
        sa.Column(
            "pool_tier",
            sa.Enum("standard", "premium", "enterprise", "god", name="pooltierenum"),
            nullable=False,
            server_default="standard"
        ),
        sa.Column(
            "status",
            sa.Enum("active", "paused", "suspended", "probation", name="poolmemberstatusenum"),
            nullable=False,
            server_default="active"
        ),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("quality_score", sa.Float(), nullable=False, server_default="50.0"),
        sa.Column("health_status", sa.String(length=20), nullable=False, server_default="healthy"),
        sa.Column("daily_send_limit", sa.Integer(), nullable=False, server_default="20"),
        sa.Column("daily_receive_limit", sa.Integer(), nullable=False, server_default="25"),
        sa.Column("sends_today", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("receives_today", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_sends", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_receives", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_opens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_replies", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_bounces", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_spam_reports", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("response_rate", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("open_rate", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("bounce_rate", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("spf_verified", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("dkim_verified", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("dmarc_verified", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("settings", sa.JSON(), nullable=True),
        sa.Column("joined_at", sa.DateTime(), nullable=True),
        sa.Column("last_activity_at", sa.DateTime(), nullable=True),
        sa.Column("last_send_at", sa.DateTime(), nullable=True),
        sa.Column("last_receive_at", sa.DateTime(), nullable=True),
        sa.Column("paused_at", sa.DateTime(), nullable=True),
        sa.Column("pause_reason", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["candidate_id"], ["candidates.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_warmup_pool_members_candidate_id", "warmup_pool_members", ["candidate_id"], unique=True)
    op.create_index("ix_warmup_pool_members_email", "warmup_pool_members", ["email"], unique=True)
    op.create_index("ix_warmup_pool_members_pool_tier", "warmup_pool_members", ["pool_tier"])
    op.create_index("ix_warmup_pool_members_status", "warmup_pool_members", ["status"])
    op.create_index("ix_warmup_pool_members_quality_score", "warmup_pool_members", ["quality_score"])

    # =========================================================================
    # warmup_conversations - Peer-to-peer warmup conversations
    # =========================================================================
    op.create_table(
        "warmup_conversations",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("thread_id", sa.String(length=100), nullable=False),
        sa.Column("sender_id", sa.Integer(), nullable=False),
        sa.Column("receiver_id", sa.Integer(), nullable=False),
        sa.Column("subject", sa.String(length=255), nullable=True),
        sa.Column("topic", sa.String(length=100), nullable=True),
        sa.Column("category", sa.String(length=50), nullable=True),
        sa.Column("tone", sa.String(length=50), nullable=True),
        sa.Column(
            "status",
            sa.Enum("scheduled", "active", "completed", "failed", name="conversationstatusenum"),
            nullable=False,
            server_default="scheduled"
        ),
        sa.Column("message_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_opened", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("is_replied", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("is_rescued_from_spam", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("landed_in_spam", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("engagement_score", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("scheduled_at", sa.DateTime(), nullable=True),
        sa.Column("sent_at", sa.DateTime(), nullable=True),
        sa.Column("opened_at", sa.DateTime(), nullable=True),
        sa.Column("replied_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["sender_id"], ["warmup_pool_members.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["receiver_id"], ["warmup_pool_members.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_warmup_conversations_thread_id", "warmup_conversations", ["thread_id"], unique=True)
    op.create_index("ix_warmup_conversations_sender_id", "warmup_conversations", ["sender_id"])
    op.create_index("ix_warmup_conversations_receiver_id", "warmup_conversations", ["receiver_id"])
    op.create_index("ix_warmup_conversations_status", "warmup_conversations", ["status"])
    op.create_index("ix_warmup_conversations_scheduled_at", "warmup_conversations", ["scheduled_at"])

    # =========================================================================
    # warmup_messages - Individual messages in conversations
    # =========================================================================
    op.create_table(
        "warmup_messages",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("conversation_id", sa.Integer(), nullable=False),
        sa.Column("message_id", sa.String(length=255), nullable=True),
        sa.Column("sender_email", sa.String(length=255), nullable=False),
        sa.Column("receiver_email", sa.String(length=255), nullable=False),
        sa.Column("subject", sa.String(length=255), nullable=True),
        sa.Column("body_text", sa.Text(), nullable=True),
        sa.Column("body_html", sa.Text(), nullable=True),
        sa.Column("is_initial", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("is_reply", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("is_ai_generated", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("sent_at", sa.DateTime(), nullable=True),
        sa.Column("delivered_at", sa.DateTime(), nullable=True),
        sa.Column("opened_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["conversation_id"], ["warmup_conversations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_warmup_messages_conversation_id", "warmup_messages", ["conversation_id"])
    op.create_index("ix_warmup_messages_message_id", "warmup_messages", ["message_id"])

    # =========================================================================
    # inbox_placement_tests - Deliverability testing
    # =========================================================================
    op.create_table(
        "inbox_placement_tests",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("test_type", sa.String(length=50), nullable=False, server_default="standard"),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("overall_score", sa.Float(), nullable=True),
        sa.Column("overall_inbox_rate", sa.Float(), nullable=True),
        sa.Column("overall_spam_rate", sa.Float(), nullable=True),
        sa.Column("overall_missing_rate", sa.Float(), nullable=True),
        sa.Column("gmail_results", sa.JSON(), nullable=True),
        sa.Column("outlook_results", sa.JSON(), nullable=True),
        sa.Column("yahoo_results", sa.JSON(), nullable=True),
        sa.Column("other_results", sa.JSON(), nullable=True),
        sa.Column("issues_detected", sa.JSON(), nullable=True),
        sa.Column("recommendations", sa.JSON(), nullable=True),
        sa.Column("test_emails_sent", sa.Integer(), nullable=True),
        sa.Column("test_emails_checked", sa.Integer(), nullable=True),
        sa.Column("test_date", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["candidate_id"], ["candidates.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_inbox_placement_tests_candidate_id", "inbox_placement_tests", ["candidate_id"])
    op.create_index("ix_inbox_placement_tests_test_date", "inbox_placement_tests", ["test_date"])

    # =========================================================================
    # placement_test_results - Individual provider results
    # =========================================================================
    op.create_table(
        "placement_test_results",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("test_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=50), nullable=False),
        sa.Column("seed_email", sa.String(length=255), nullable=True),
        sa.Column(
            "placement",
            sa.Enum("inbox", "spam", "promotions", "updates", "missing", name="placementresultenum"),
            nullable=True
        ),
        sa.Column("headers_analysis", sa.JSON(), nullable=True),
        sa.Column("spam_score", sa.Float(), nullable=True),
        sa.Column("check_time", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["test_id"], ["inbox_placement_tests.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_placement_test_results_test_id", "placement_test_results", ["test_id"])
    op.create_index("ix_placement_test_results_provider", "placement_test_results", ["provider"])

    # =========================================================================
    # blacklist_status - Blacklist monitoring
    # =========================================================================
    op.create_table(
        "blacklist_status",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("ip_address", sa.String(length=45), nullable=True),
        sa.Column("domain", sa.String(length=255), nullable=True),
        sa.Column("is_listed_anywhere", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column(
            "severity",
            sa.Enum("none", "low", "medium", "high", "critical", name="blacklistseverityenum"),
            nullable=False,
            server_default="none"
        ),
        sa.Column("total_blacklists_checked", sa.Integer(), nullable=True),
        sa.Column("total_listings", sa.Integer(), nullable=True),
        sa.Column("spamhaus", sa.Boolean(), nullable=True),
        sa.Column("spamhaus_details", sa.JSON(), nullable=True),
        sa.Column("barracuda", sa.Boolean(), nullable=True),
        sa.Column("barracuda_details", sa.JSON(), nullable=True),
        sa.Column("sorbs", sa.Boolean(), nullable=True),
        sa.Column("sorbs_details", sa.JSON(), nullable=True),
        sa.Column("spamcop", sa.Boolean(), nullable=True),
        sa.Column("spamcop_details", sa.JSON(), nullable=True),
        sa.Column("other_listings", sa.JSON(), nullable=True),
        sa.Column("new_listings", sa.JSON(), nullable=True),
        sa.Column("removed_listings", sa.JSON(), nullable=True),
        sa.Column("check_date", sa.DateTime(), nullable=True),
        sa.Column("next_check_date", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["candidate_id"], ["candidates.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_blacklist_status_candidate_id", "blacklist_status", ["candidate_id"])
    op.create_index("ix_blacklist_status_check_date", "blacklist_status", ["check_date"])
    op.create_index("ix_blacklist_status_severity", "blacklist_status", ["severity"])

    # =========================================================================
    # warmup_schedules - Schedule configuration
    # =========================================================================
    op.create_table(
        "warmup_schedules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("member_id", sa.Integer(), nullable=True),
        sa.Column("timezone", sa.String(length=50), nullable=False, server_default="UTC"),
        sa.Column("start_hour", sa.Integer(), nullable=False, server_default="9"),
        sa.Column("end_hour", sa.Integer(), nullable=False, server_default="17"),
        sa.Column("active_days", sa.Integer(), nullable=False, server_default="31"),  # Mon-Fri bitmask
        sa.Column("weekdays_only", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("min_delay_between_sends", sa.Integer(), nullable=False, server_default="300"),
        sa.Column("max_delay_between_sends", sa.Integer(), nullable=False, server_default="1800"),
        sa.Column("ramp_up_enabled", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("current_ramp_day", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("ramp_up_days", sa.Integer(), nullable=False, server_default="30"),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["candidate_id"], ["candidates.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["member_id"], ["warmup_pool_members.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_warmup_schedules_candidate_id", "warmup_schedules", ["candidate_id"], unique=True)
    op.create_index("ix_warmup_schedules_member_id", "warmup_schedules", ["member_id"])

    # =========================================================================
    # spam_rescue_logs - Spam rescue tracking
    # =========================================================================
    op.create_table(
        "spam_rescue_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("member_id", sa.Integer(), nullable=False),
        sa.Column("conversation_id", sa.Integer(), nullable=True),
        sa.Column("original_folder", sa.String(length=50), nullable=True),
        sa.Column("rescued_to", sa.String(length=50), nullable=True),
        sa.Column("rescue_method", sa.String(length=50), nullable=True),
        sa.Column("was_marked_important", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("was_replied", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("spam_reason", sa.String(length=255), nullable=True),
        sa.Column("rescue_success", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("detected_at", sa.DateTime(), nullable=True),
        sa.Column("rescued_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["member_id"], ["warmup_pool_members.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["conversation_id"], ["warmup_conversations.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_spam_rescue_logs_member_id", "spam_rescue_logs", ["member_id"])
    op.create_index("ix_spam_rescue_logs_rescued_at", "spam_rescue_logs", ["rescued_at"])


def downgrade() -> None:
    """Downgrade schema - Remove warmup pool system tables."""
    op.drop_table("spam_rescue_logs")
    op.drop_table("warmup_schedules")
    op.drop_table("blacklist_status")
    op.drop_table("placement_test_results")
    op.drop_table("inbox_placement_tests")
    op.drop_table("warmup_messages")
    op.drop_table("warmup_conversations")
    op.drop_table("warmup_pool_members")

    # Drop enums
    op.execute("DROP TYPE IF EXISTS pooltierenum")
    op.execute("DROP TYPE IF EXISTS poolmemberstatusenum")
    op.execute("DROP TYPE IF EXISTS conversationstatusenum")
    op.execute("DROP TYPE IF EXISTS placementresultenum")
    op.execute("DROP TYPE IF EXISTS blacklistseverityenum")
