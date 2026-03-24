"""Add follow-up campaign integration to group campaigns

Revision ID: i5h6a7b8c9d0
Revises: h4g5a6b7c8d9
Create Date: 2026-02-03 10:00:00.000000

Adds follow-up configuration to group_campaigns table and
extends follow_up_campaigns to support group campaign recipients.

Changes:
1. group_campaigns: Add enable_follow_up, follow_up_sequence_id,
   follow_up_stop_on_reply, follow_up_stop_on_bounce
2. follow_up_campaigns: Make application_id nullable, add
   group_campaign_recipient_id and group_campaign_id
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "i5h6a7b8c9d0"
down_revision: Union[str, Sequence[str], None] = "h4g5a6b7c8d9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add follow-up integration columns."""

    # =========================================================================
    # group_campaigns - Add follow-up configuration
    # =========================================================================
    op.add_column(
        "group_campaigns",
        sa.Column("enable_follow_up", sa.Boolean(), nullable=False, server_default="0")
    )
    op.add_column(
        "group_campaigns",
        sa.Column("follow_up_sequence_id", sa.Integer(), nullable=True)
    )
    op.add_column(
        "group_campaigns",
        sa.Column("follow_up_stop_on_reply", sa.Boolean(), nullable=False, server_default="1")
    )
    op.add_column(
        "group_campaigns",
        sa.Column("follow_up_stop_on_bounce", sa.Boolean(), nullable=False, server_default="0")
    )

    # Add foreign key constraint for follow_up_sequence_id
    op.create_foreign_key(
        "fk_group_campaigns_follow_up_sequence",
        "group_campaigns",
        "follow_up_sequences",
        ["follow_up_sequence_id"],
        ["id"],
        ondelete="SET NULL"
    )

    # Add index for follow-up enabled campaigns
    op.create_index(
        "ix_group_campaigns_follow_up",
        "group_campaigns",
        ["enable_follow_up", "follow_up_sequence_id"],
        unique=False
    )

    # =========================================================================
    # follow_up_campaigns - Extend for group campaign support
    # =========================================================================

    # Make application_id nullable (to support group campaign recipients)
    op.alter_column(
        "follow_up_campaigns",
        "application_id",
        existing_type=sa.Integer(),
        nullable=True
    )

    # Add group_campaign_recipient_id
    op.add_column(
        "follow_up_campaigns",
        sa.Column("group_campaign_recipient_id", sa.Integer(), nullable=True)
    )

    # Add group_campaign_id
    op.add_column(
        "follow_up_campaigns",
        sa.Column("group_campaign_id", sa.Integer(), nullable=True)
    )

    # Add foreign key constraints
    op.create_foreign_key(
        "fk_follow_up_campaigns_group_recipient",
        "follow_up_campaigns",
        "group_campaign_recipients",
        ["group_campaign_recipient_id"],
        ["id"],
        ondelete="CASCADE"
    )

    op.create_foreign_key(
        "fk_follow_up_campaigns_group_campaign",
        "follow_up_campaigns",
        "group_campaigns",
        ["group_campaign_id"],
        ["id"],
        ondelete="CASCADE"
    )

    # Add indexes for efficient querying
    op.create_index(
        "ix_follow_up_campaigns_group_recipient",
        "follow_up_campaigns",
        ["group_campaign_recipient_id"],
        unique=False
    )

    op.create_index(
        "ix_follow_up_campaigns_group_campaign",
        "follow_up_campaigns",
        ["group_campaign_id"],
        unique=False
    )


def downgrade() -> None:
    """Remove follow-up integration columns."""

    # =========================================================================
    # follow_up_campaigns - Remove group campaign support
    # =========================================================================

    # Drop indexes
    op.drop_index("ix_follow_up_campaigns_group_campaign", table_name="follow_up_campaigns")
    op.drop_index("ix_follow_up_campaigns_group_recipient", table_name="follow_up_campaigns")

    # Drop foreign keys
    op.drop_constraint("fk_follow_up_campaigns_group_campaign", "follow_up_campaigns", type_="foreignkey")
    op.drop_constraint("fk_follow_up_campaigns_group_recipient", "follow_up_campaigns", type_="foreignkey")

    # Drop columns
    op.drop_column("follow_up_campaigns", "group_campaign_id")
    op.drop_column("follow_up_campaigns", "group_campaign_recipient_id")

    # Revert application_id to non-nullable (may fail if data exists)
    op.alter_column(
        "follow_up_campaigns",
        "application_id",
        existing_type=sa.Integer(),
        nullable=False
    )

    # =========================================================================
    # group_campaigns - Remove follow-up configuration
    # =========================================================================

    # Drop index
    op.drop_index("ix_group_campaigns_follow_up", table_name="group_campaigns")

    # Drop foreign key
    op.drop_constraint("fk_group_campaigns_follow_up_sequence", "group_campaigns", type_="foreignkey")

    # Drop columns
    op.drop_column("group_campaigns", "follow_up_stop_on_bounce")
    op.drop_column("group_campaigns", "follow_up_stop_on_reply")
    op.drop_column("group_campaigns", "follow_up_sequence_id")
    op.drop_column("group_campaigns", "enable_follow_up")
