"""add_recipient_groups_system

Revision ID: 6598d87d92e2
Revises: 8950aca76f7a
Create Date: 2026-01-14 12:17:43.023761

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6598d87d92e2'
down_revision: Union[str, Sequence[str], None] = '8950aca76f7a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create recipients table
    op.create_table(
        'recipients',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=True),
        sa.Column('company', sa.String(length=255), nullable=True),
        sa.Column('position', sa.String(length=255), nullable=True),
        sa.Column('country', sa.String(length=100), nullable=True),
        sa.Column('language', sa.String(length=50), nullable=True),
        sa.Column('tags', sa.String(length=500), nullable=True),
        sa.Column('source', sa.String(length=100), nullable=True),
        sa.Column('custom_fields', sa.JSON(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('unsubscribed', sa.Boolean(), nullable=True),
        sa.Column('total_emails_sent', sa.Integer(), nullable=True),
        sa.Column('total_emails_opened', sa.Integer(), nullable=True),
        sa.Column('total_emails_replied', sa.Integer(), nullable=True),
        sa.Column('engagement_score', sa.Float(), nullable=True),
        sa.Column('deleted_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_recipients_candidate_id'), 'recipients', ['candidate_id'], unique=False)
    op.create_index(op.f('ix_recipients_company'), 'recipients', ['company'], unique=False)
    op.create_index(op.f('ix_recipients_country'), 'recipients', ['country'], unique=False)
    op.create_index(op.f('ix_recipients_deleted_at'), 'recipients', ['deleted_at'], unique=False)
    op.create_index(op.f('ix_recipients_email'), 'recipients', ['email'], unique=False)
    op.create_index(op.f('ix_recipients_id'), 'recipients', ['id'], unique=False)
    op.create_index(op.f('ix_recipients_is_active'), 'recipients', ['is_active'], unique=False)
    op.create_index(op.f('ix_recipients_unsubscribed'), 'recipients', ['unsubscribed'], unique=False)
    op.create_index('ix_recipient_candidate_email', 'recipients', ['candidate_id', 'email'], unique=True)
    op.create_index('ix_recipient_engagement', 'recipients', ['candidate_id', 'engagement_score'])

    # Create recipient_groups table
    op.create_table(
        'recipient_groups',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('group_type', sa.Enum('STATIC', 'DYNAMIC', name='grouptypeenum'), nullable=False),
        sa.Column('filter_criteria', sa.JSON(), nullable=True),
        sa.Column('auto_refresh', sa.Boolean(), nullable=True),
        sa.Column('last_refreshed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('total_recipients', sa.Integer(), nullable=True),
        sa.Column('active_recipients', sa.Integer(), nullable=True),
        sa.Column('color', sa.String(length=50), nullable=True),
        sa.Column('deleted_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_recipient_groups_candidate_id'), 'recipient_groups', ['candidate_id'], unique=False)
    op.create_index(op.f('ix_recipient_groups_deleted_at'), 'recipient_groups', ['deleted_at'], unique=False)
    op.create_index(op.f('ix_recipient_groups_group_type'), 'recipient_groups', ['group_type'], unique=False)
    op.create_index(op.f('ix_recipient_groups_id'), 'recipient_groups', ['id'], unique=False)
    op.create_index(op.f('ix_recipient_groups_name'), 'recipient_groups', ['name'], unique=False)
    op.create_index('ix_group_candidate_name', 'recipient_groups', ['candidate_id', 'name'], unique=True)

    # Create group_recipients junction table
    op.create_table(
        'group_recipients',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('group_id', sa.Integer(), nullable=False),
        sa.Column('recipient_id', sa.Integer(), nullable=False),
        sa.Column('is_dynamic_membership', sa.Boolean(), nullable=True),
        sa.Column('added_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.ForeignKeyConstraint(['group_id'], ['recipient_groups.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['recipient_id'], ['recipients.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_group_recipients_group_id'), 'group_recipients', ['group_id'], unique=False)
    op.create_index(op.f('ix_group_recipients_id'), 'group_recipients', ['id'], unique=False)
    op.create_index(op.f('ix_group_recipients_recipient_id'), 'group_recipients', ['recipient_id'], unique=False)
    op.create_index('ix_group_recipient_unique', 'group_recipients', ['group_id', 'recipient_id'], unique=True)
    op.create_index('ix_group_recipient_group', 'group_recipients', ['group_id', 'recipient_id'])

    # Create group_campaigns table
    op.create_table(
        'group_campaigns',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('group_id', sa.Integer(), nullable=True),
        sa.Column('campaign_name', sa.String(length=255), nullable=False),
        sa.Column('email_template_id', sa.Integer(), nullable=True),
        sa.Column('subject_template', sa.String(length=500), nullable=False),
        sa.Column('body_template_html', sa.Text(), nullable=False),
        sa.Column('send_delay_seconds', sa.Integer(), nullable=True),
        sa.Column('scheduled_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('status', sa.Enum('DRAFT', 'SCHEDULED', 'SENDING', 'COMPLETED', 'FAILED', 'PAUSED', 'CANCELLED', name='campaignstatusenum'), nullable=True),
        sa.Column('total_recipients', sa.Integer(), nullable=True),
        sa.Column('sent_count', sa.Integer(), nullable=True),
        sa.Column('failed_count', sa.Integer(), nullable=True),
        sa.Column('skipped_count', sa.Integer(), nullable=True),
        sa.Column('opened_count', sa.Integer(), nullable=True),
        sa.Column('replied_count', sa.Integer(), nullable=True),
        sa.Column('bounced_count', sa.Integer(), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('paused_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('deleted_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['email_template_id'], ['email_templates.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['group_id'], ['recipient_groups.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_group_campaigns_campaign_name'), 'group_campaigns', ['campaign_name'], unique=False)
    op.create_index(op.f('ix_group_campaigns_candidate_id'), 'group_campaigns', ['candidate_id'], unique=False)
    op.create_index(op.f('ix_group_campaigns_deleted_at'), 'group_campaigns', ['deleted_at'], unique=False)
    op.create_index(op.f('ix_group_campaigns_group_id'), 'group_campaigns', ['group_id'], unique=False)
    op.create_index(op.f('ix_group_campaigns_id'), 'group_campaigns', ['id'], unique=False)
    op.create_index(op.f('ix_group_campaigns_status'), 'group_campaigns', ['status'], unique=False)
    op.create_index('ix_campaign_candidate_status', 'group_campaigns', ['candidate_id', 'status', 'deleted_at'])
    op.create_index('ix_campaign_group_created', 'group_campaigns', ['group_id', 'created_at'])
    op.create_index('ix_campaign_scheduled', 'group_campaigns', ['scheduled_at', 'status'])

    # Create group_campaign_recipients table
    op.create_table(
        'group_campaign_recipients',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('campaign_id', sa.Integer(), nullable=False),
        sa.Column('recipient_id', sa.Integer(), nullable=False),
        sa.Column('rendered_subject', sa.String(length=500), nullable=True),
        sa.Column('rendered_body_html', sa.Text(), nullable=True),
        sa.Column('status', sa.Enum('PENDING', 'SENT', 'FAILED', 'SKIPPED', 'OPENED', 'REPLIED', 'BOUNCED', name='recipientstatusenum'), nullable=True),
        sa.Column('tracking_id', sa.String(length=100), nullable=True),
        sa.Column('sent_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('delivered_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('opened_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('replied_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('bounced_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('retry_count', sa.Integer(), nullable=True),
        sa.Column('email_log_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['campaign_id'], ['group_campaigns.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['email_log_id'], ['email_logs.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['recipient_id'], ['recipients.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_group_campaign_recipients_campaign_id'), 'group_campaign_recipients', ['campaign_id'], unique=False)
    op.create_index(op.f('ix_group_campaign_recipients_id'), 'group_campaign_recipients', ['id'], unique=False)
    op.create_index(op.f('ix_group_campaign_recipients_recipient_id'), 'group_campaign_recipients', ['recipient_id'], unique=False)
    op.create_index(op.f('ix_group_campaign_recipients_status'), 'group_campaign_recipients', ['status'], unique=False)
    op.create_index(op.f('ix_group_campaign_recipients_tracking_id'), 'group_campaign_recipients', ['tracking_id'], unique=True)
    op.create_index('ix_campaign_recipient_unique', 'group_campaign_recipients', ['campaign_id', 'recipient_id'], unique=True)
    op.create_index('ix_campaign_recipient_status', 'group_campaign_recipients', ['campaign_id', 'status'])
    op.create_index('ix_recipient_campaign_sent', 'group_campaign_recipients', ['recipient_id', 'sent_at'])


def downgrade() -> None:
    """Downgrade schema."""
    # Drop tables in reverse order
    op.drop_index('ix_recipient_campaign_sent', table_name='group_campaign_recipients')
    op.drop_index('ix_campaign_recipient_status', table_name='group_campaign_recipients')
    op.drop_index('ix_campaign_recipient_unique', table_name='group_campaign_recipients')
    op.drop_index(op.f('ix_group_campaign_recipients_tracking_id'), table_name='group_campaign_recipients')
    op.drop_index(op.f('ix_group_campaign_recipients_status'), table_name='group_campaign_recipients')
    op.drop_index(op.f('ix_group_campaign_recipients_recipient_id'), table_name='group_campaign_recipients')
    op.drop_index(op.f('ix_group_campaign_recipients_id'), table_name='group_campaign_recipients')
    op.drop_index(op.f('ix_group_campaign_recipients_campaign_id'), table_name='group_campaign_recipients')
    op.drop_table('group_campaign_recipients')

    op.drop_index('ix_campaign_scheduled', table_name='group_campaigns')
    op.drop_index('ix_campaign_group_created', table_name='group_campaigns')
    op.drop_index('ix_campaign_candidate_status', table_name='group_campaigns')
    op.drop_index(op.f('ix_group_campaigns_status'), table_name='group_campaigns')
    op.drop_index(op.f('ix_group_campaigns_id'), table_name='group_campaigns')
    op.drop_index(op.f('ix_group_campaigns_group_id'), table_name='group_campaigns')
    op.drop_index(op.f('ix_group_campaigns_deleted_at'), table_name='group_campaigns')
    op.drop_index(op.f('ix_group_campaigns_candidate_id'), table_name='group_campaigns')
    op.drop_index(op.f('ix_group_campaigns_campaign_name'), table_name='group_campaigns')
    op.drop_table('group_campaigns')

    op.drop_index('ix_group_recipient_group', table_name='group_recipients')
    op.drop_index('ix_group_recipient_unique', table_name='group_recipients')
    op.drop_index(op.f('ix_group_recipients_recipient_id'), table_name='group_recipients')
    op.drop_index(op.f('ix_group_recipients_id'), table_name='group_recipients')
    op.drop_index(op.f('ix_group_recipients_group_id'), table_name='group_recipients')
    op.drop_table('group_recipients')

    op.drop_index('ix_group_candidate_name', table_name='recipient_groups')
    op.drop_index(op.f('ix_recipient_groups_name'), table_name='recipient_groups')
    op.drop_index(op.f('ix_recipient_groups_id'), table_name='recipient_groups')
    op.drop_index(op.f('ix_recipient_groups_group_type'), table_name='recipient_groups')
    op.drop_index(op.f('ix_recipient_groups_deleted_at'), table_name='recipient_groups')
    op.drop_index(op.f('ix_recipient_groups_candidate_id'), table_name='recipient_groups')
    op.drop_table('recipient_groups')

    op.drop_index('ix_recipient_engagement', table_name='recipients')
    op.drop_index('ix_recipient_candidate_email', table_name='recipients')
    op.drop_index(op.f('ix_recipients_unsubscribed'), table_name='recipients')
    op.drop_index(op.f('ix_recipients_is_active'), table_name='recipients')
    op.drop_index(op.f('ix_recipients_id'), table_name='recipients')
    op.drop_index(op.f('ix_recipients_email'), table_name='recipients')
    op.drop_index(op.f('ix_recipients_deleted_at'), table_name='recipients')
    op.drop_index(op.f('ix_recipients_country'), table_name='recipients')
    op.drop_index(op.f('ix_recipients_company'), table_name='recipients')
    op.drop_index(op.f('ix_recipients_candidate_id'), table_name='recipients')
    op.drop_table('recipients')

    # Drop enum types
    op.execute('DROP TYPE IF EXISTS recipientstatusenum')
    op.execute('DROP TYPE IF EXISTS campaignstatusenum')
    op.execute('DROP TYPE IF EXISTS grouptypeenum')
