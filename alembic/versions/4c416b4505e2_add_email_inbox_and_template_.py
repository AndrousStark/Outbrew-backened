"""add_email_inbox_and_template_marketplace_tables

Revision ID: 4c416b4505e2
Revises: c3a4b5d6e7f8
Create Date: 2026-01-12 14:48:28.643292

This migration adds:
EMAIL INBOX TABLES:
1. email_accounts - IMAP email account configurations
2. email_messages - Sent and received email messages
3. email_threads - Email conversation threads
4. storage_quotas - Storage quota tracking per candidate

TEMPLATE MARKETPLACE TABLES:
5. public_templates - Shared community templates
6. template_ratings - User ratings for templates (1-5 stars)
7. template_reviews - Detailed template reviews
8. template_usage_reports - Anonymous usage statistics
9. template_favorites - User bookmarked templates
10. template_collections - Curated template collections
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4c416b4505e2'
down_revision: Union[str, Sequence[str], None] = 'c3a4b5d6e7f8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Add Email Inbox and Template Marketplace tables."""

    # ===========================================
    # EMAIL INBOX TABLES
    # ===========================================

    # 1. CREATE EMAIL_ACCOUNTS TABLE
    op.create_table('email_accounts',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('email_address', sa.String(length=255), nullable=False),
        sa.Column('display_name', sa.String(length=255), nullable=True),
        sa.Column('provider', sa.String(length=50), nullable=True),
        sa.Column('imap_host', sa.String(length=255), nullable=False),
        sa.Column('imap_port', sa.Integer(), nullable=False),
        sa.Column('imap_username', sa.String(length=255), nullable=False),
        sa.Column('imap_password', sa.Text(), nullable=False),
        sa.Column('use_ssl', sa.Boolean(), nullable=False, server_default='1'),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='1'),
        sa.Column('last_sync_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('sync_frequency_minutes', sa.Integer(), nullable=False, server_default='30'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('deleted_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_email_accounts_id'), 'email_accounts', ['id'], unique=False)
    op.create_index(op.f('ix_email_accounts_candidate_id'), 'email_accounts', ['candidate_id'], unique=False)
    op.create_index(op.f('ix_email_accounts_email_address'), 'email_accounts', ['email_address'], unique=False)
    op.create_index('ix_email_accounts_candidate_email', 'email_accounts', ['candidate_id', 'email_address'], unique=True)

    # 2. CREATE EMAIL_THREADS TABLE
    op.create_table('email_threads',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('thread_id', sa.String(length=255), nullable=False),
        sa.Column('subject', sa.String(length=500), nullable=True),
        sa.Column('participants', sa.Text(), nullable=True),
        sa.Column('message_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('unread_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('is_starred', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('latest_message_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('latest_snippet', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_email_threads_id'), 'email_threads', ['id'], unique=False)
    op.create_index(op.f('ix_email_threads_candidate_id'), 'email_threads', ['candidate_id'], unique=False)
    op.create_index(op.f('ix_email_threads_thread_id'), 'email_threads', ['thread_id'], unique=False)
    op.create_index('ix_email_threads_candidate_thread', 'email_threads', ['candidate_id', 'thread_id'], unique=True)
    op.create_index('ix_email_threads_latest_message', 'email_threads', ['latest_message_at'], unique=False)

    # 3. CREATE EMAIL_MESSAGES TABLE
    op.create_table('email_messages',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('email_account_id', sa.Integer(), nullable=True),
        sa.Column('application_id', sa.Integer(), nullable=True),
        sa.Column('direction', sa.String(length=20), nullable=False),
        sa.Column('message_id', sa.String(length=500), nullable=True),
        sa.Column('in_reply_to', sa.String(length=500), nullable=True),
        sa.Column('thread_id', sa.String(length=255), nullable=False),
        sa.Column('from_email', sa.String(length=255), nullable=False),
        sa.Column('from_name', sa.String(length=255), nullable=True),
        sa.Column('to_email', sa.String(length=255), nullable=False),
        sa.Column('to_name', sa.String(length=255), nullable=True),
        sa.Column('subject', sa.String(length=500), nullable=True),
        sa.Column('body_text', sa.Text(), nullable=True),
        sa.Column('body_html', sa.Text(), nullable=True),
        sa.Column('snippet', sa.Text(), nullable=True),
        sa.Column('file_path', sa.String(length=500), nullable=True),
        sa.Column('is_read', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('is_starred', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('is_important', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('is_archived', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('is_spam', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('is_trash', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('opened_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('clicked_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('bounced', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('bounce_reason', sa.Text(), nullable=True),
        sa.Column('size_bytes', sa.Integer(), nullable=True),
        sa.Column('sent_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('received_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.Column('deleted_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['email_account_id'], ['email_accounts.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['application_id'], ['applications.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_email_messages_id'), 'email_messages', ['id'], unique=False)
    op.create_index(op.f('ix_email_messages_candidate_id'), 'email_messages', ['candidate_id'], unique=False)
    op.create_index(op.f('ix_email_messages_email_account_id'), 'email_messages', ['email_account_id'], unique=False)
    op.create_index(op.f('ix_email_messages_application_id'), 'email_messages', ['application_id'], unique=False)
    op.create_index(op.f('ix_email_messages_thread_id'), 'email_messages', ['thread_id'], unique=False)
    op.create_index(op.f('ix_email_messages_message_id'), 'email_messages', ['message_id'], unique=True)
    op.create_index(op.f('ix_email_messages_direction'), 'email_messages', ['direction'], unique=False)
    op.create_index(op.f('ix_email_messages_is_read'), 'email_messages', ['is_read'], unique=False)
    op.create_index('ix_email_messages_candidate_thread', 'email_messages', ['candidate_id', 'thread_id'], unique=False)

    # 4. CREATE STORAGE_QUOTAS TABLE
    op.create_table('storage_quotas',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('quota_limit', sa.BigInteger(), nullable=False, server_default='524288000'),
        sa.Column('used_bytes', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('resumes_bytes', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('emails_bytes', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('documents_bytes', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('templates_bytes', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('total_files', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('total_emails_archived', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_calculated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_storage_quotas_id'), 'storage_quotas', ['id'], unique=False)
    op.create_index(op.f('ix_storage_quotas_candidate_id'), 'storage_quotas', ['candidate_id'], unique=True)

    # ===========================================
    # TEMPLATE MARKETPLACE TABLES
    # ===========================================

    # 5. CREATE PUBLIC_TEMPLATES TABLE
    op.create_table('public_templates',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('creator_id', sa.Integer(), nullable=False),
        sa.Column('creator_name', sa.String(length=255), nullable=True),
        sa.Column('source_template_id', sa.Integer(), nullable=True),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('category', sa.String(length=50), nullable=False),
        sa.Column('language', sa.String(length=50), nullable=False, server_default='en'),
        sa.Column('tags', sa.Text(), nullable=True),
        sa.Column('subject_template', sa.Text(), nullable=False),
        sa.Column('body_template_html', sa.Text(), nullable=False),
        sa.Column('variables', sa.Text(), nullable=True),
        sa.Column('preview_text', sa.Text(), nullable=True),
        sa.Column('target_industry', sa.String(length=100), nullable=True),
        sa.Column('target_role', sa.String(length=100), nullable=True),
        sa.Column('target_country', sa.String(length=100), nullable=True),
        sa.Column('visibility', sa.String(length=20), nullable=False, server_default='PUBLIC'),
        sa.Column('is_featured', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('is_approved', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('is_flagged', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('flag_reason', sa.Text(), nullable=True),
        sa.Column('moderated_by', sa.Integer(), nullable=True),
        sa.Column('moderated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('total_clones', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('total_uses', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('total_views', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('average_rating', sa.Float(), nullable=True),
        sa.Column('rating_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('response_rate', sa.Float(), nullable=True),
        sa.Column('avg_response_time_hours', sa.Float(), nullable=True),
        sa.Column('email_opens', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('email_clicks', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('email_replies', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('deleted_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['creator_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['source_template_id'], ['email_templates.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['moderated_by'], ['candidates.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_public_templates_id'), 'public_templates', ['id'], unique=False)
    op.create_index(op.f('ix_public_templates_creator_id'), 'public_templates', ['creator_id'], unique=False)
    op.create_index(op.f('ix_public_templates_category'), 'public_templates', ['category'], unique=False)
    op.create_index(op.f('ix_public_templates_language'), 'public_templates', ['language'], unique=False)
    op.create_index(op.f('ix_public_templates_visibility'), 'public_templates', ['visibility'], unique=False)
    op.create_index(op.f('ix_public_templates_is_featured'), 'public_templates', ['is_featured'], unique=False)
    op.create_index(op.f('ix_public_templates_is_approved'), 'public_templates', ['is_approved'], unique=False)
    op.create_index('ix_public_templates_created_at', 'public_templates', ['created_at'], unique=False)
    op.create_index('ix_public_templates_average_rating', 'public_templates', ['average_rating'], unique=False)

    # 6. CREATE TEMPLATE_RATINGS TABLE
    op.create_table('template_ratings',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('template_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('rating', sa.Integer(), nullable=False),
        sa.Column('times_used', sa.Integer(), nullable=True),
        sa.Column('got_response', sa.Boolean(), nullable=True),
        sa.Column('response_time_hours', sa.Integer(), nullable=True),
        sa.Column('industry', sa.String(length=100), nullable=True),
        sa.Column('role', sa.String(length=100), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['template_id'], ['public_templates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.CheckConstraint('rating >= 1 AND rating <= 5', name='check_rating_range')
    )
    op.create_index(op.f('ix_template_ratings_id'), 'template_ratings', ['id'], unique=False)
    op.create_index(op.f('ix_template_ratings_template_id'), 'template_ratings', ['template_id'], unique=False)
    op.create_index(op.f('ix_template_ratings_user_id'), 'template_ratings', ['user_id'], unique=False)
    op.create_index('ix_template_ratings_user_template', 'template_ratings', ['user_id', 'template_id'], unique=True)

    # 7. CREATE TEMPLATE_REVIEWS TABLE
    op.create_table('template_reviews',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('template_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('rating_id', sa.Integer(), nullable=True),
        sa.Column('title', sa.String(length=255), nullable=True),
        sa.Column('review_text', sa.Text(), nullable=False),
        sa.Column('pros', sa.Text(), nullable=True),
        sa.Column('cons', sa.Text(), nullable=True),
        sa.Column('emails_sent', sa.Integer(), nullable=True),
        sa.Column('responses_received', sa.Integer(), nullable=True),
        sa.Column('would_recommend', sa.Boolean(), nullable=True),
        sa.Column('helpful_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('is_verified_user', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('is_flagged', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['template_id'], ['public_templates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['rating_id'], ['template_ratings.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_template_reviews_id'), 'template_reviews', ['id'], unique=False)
    op.create_index(op.f('ix_template_reviews_template_id'), 'template_reviews', ['template_id'], unique=False)
    op.create_index(op.f('ix_template_reviews_user_id'), 'template_reviews', ['user_id'], unique=False)
    op.create_index('ix_template_reviews_created_at', 'template_reviews', ['created_at'], unique=False)
    op.create_index('ix_template_reviews_helpful_count', 'template_reviews', ['helpful_count'], unique=False)

    # 8. CREATE TEMPLATE_USAGE_REPORTS TABLE
    op.create_table('template_usage_reports',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('template_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('times_used', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('emails_opened', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('emails_clicked', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('emails_replied', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('got_interview', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('got_response', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('industry', sa.String(length=100), nullable=True),
        sa.Column('role', sa.String(length=100), nullable=True),
        sa.Column('country', sa.String(length=100), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['template_id'], ['public_templates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_template_usage_reports_id'), 'template_usage_reports', ['id'], unique=False)
    op.create_index(op.f('ix_template_usage_reports_template_id'), 'template_usage_reports', ['template_id'], unique=False)
    op.create_index(op.f('ix_template_usage_reports_user_id'), 'template_usage_reports', ['user_id'], unique=False)
    op.create_index('ix_template_usage_user_template', 'template_usage_reports', ['user_id', 'template_id'], unique=True)

    # 9. CREATE TEMPLATE_FAVORITES TABLE
    op.create_table('template_favorites',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('template_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.ForeignKeyConstraint(['template_id'], ['public_templates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_template_favorites_id'), 'template_favorites', ['id'], unique=False)
    op.create_index(op.f('ix_template_favorites_template_id'), 'template_favorites', ['template_id'], unique=False)
    op.create_index(op.f('ix_template_favorites_user_id'), 'template_favorites', ['user_id'], unique=False)
    op.create_index('ix_template_favorites_user_template', 'template_favorites', ['user_id', 'template_id'], unique=True)

    # 10. CREATE TEMPLATE_COLLECTIONS TABLE
    op.create_table('template_collections',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('creator_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_public', sa.Boolean(), nullable=False, server_default='1'),
        sa.Column('template_ids', sa.Text(), nullable=True),
        sa.Column('view_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('follower_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['creator_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_template_collections_id'), 'template_collections', ['id'], unique=False)
    op.create_index(op.f('ix_template_collections_creator_id'), 'template_collections', ['creator_id'], unique=False)
    op.create_index(op.f('ix_template_collections_is_public'), 'template_collections', ['is_public'], unique=False)
    op.create_index('ix_template_collections_created_at', 'template_collections', ['created_at'], unique=False)


def downgrade() -> None:
    """Downgrade schema - Drop Email Inbox and Template Marketplace tables."""

    # Drop Template Marketplace tables (reverse order)
    op.drop_index('ix_template_collections_created_at', table_name='template_collections')
    op.drop_index(op.f('ix_template_collections_is_public'), table_name='template_collections')
    op.drop_index(op.f('ix_template_collections_creator_id'), table_name='template_collections')
    op.drop_index(op.f('ix_template_collections_id'), table_name='template_collections')
    op.drop_table('template_collections')

    op.drop_index('ix_template_favorites_user_template', table_name='template_favorites')
    op.drop_index(op.f('ix_template_favorites_user_id'), table_name='template_favorites')
    op.drop_index(op.f('ix_template_favorites_template_id'), table_name='template_favorites')
    op.drop_index(op.f('ix_template_favorites_id'), table_name='template_favorites')
    op.drop_table('template_favorites')

    op.drop_index('ix_template_usage_user_template', table_name='template_usage_reports')
    op.drop_index(op.f('ix_template_usage_reports_user_id'), table_name='template_usage_reports')
    op.drop_index(op.f('ix_template_usage_reports_template_id'), table_name='template_usage_reports')
    op.drop_index(op.f('ix_template_usage_reports_id'), table_name='template_usage_reports')
    op.drop_table('template_usage_reports')

    op.drop_index('ix_template_reviews_helpful_count', table_name='template_reviews')
    op.drop_index('ix_template_reviews_created_at', table_name='template_reviews')
    op.drop_index(op.f('ix_template_reviews_user_id'), table_name='template_reviews')
    op.drop_index(op.f('ix_template_reviews_template_id'), table_name='template_reviews')
    op.drop_index(op.f('ix_template_reviews_id'), table_name='template_reviews')
    op.drop_table('template_reviews')

    op.drop_index('ix_template_ratings_user_template', table_name='template_ratings')
    op.drop_index(op.f('ix_template_ratings_user_id'), table_name='template_ratings')
    op.drop_index(op.f('ix_template_ratings_template_id'), table_name='template_ratings')
    op.drop_index(op.f('ix_template_ratings_id'), table_name='template_ratings')
    op.drop_table('template_ratings')

    op.drop_index('ix_public_templates_average_rating', table_name='public_templates')
    op.drop_index('ix_public_templates_created_at', table_name='public_templates')
    op.drop_index(op.f('ix_public_templates_is_approved'), table_name='public_templates')
    op.drop_index(op.f('ix_public_templates_is_featured'), table_name='public_templates')
    op.drop_index(op.f('ix_public_templates_visibility'), table_name='public_templates')
    op.drop_index(op.f('ix_public_templates_language'), table_name='public_templates')
    op.drop_index(op.f('ix_public_templates_category'), table_name='public_templates')
    op.drop_index(op.f('ix_public_templates_creator_id'), table_name='public_templates')
    op.drop_index(op.f('ix_public_templates_id'), table_name='public_templates')
    op.drop_table('public_templates')

    # Drop Email Inbox tables (reverse order)
    op.drop_index(op.f('ix_storage_quotas_candidate_id'), table_name='storage_quotas')
    op.drop_index(op.f('ix_storage_quotas_id'), table_name='storage_quotas')
    op.drop_table('storage_quotas')

    op.drop_index('ix_email_messages_candidate_thread', table_name='email_messages')
    op.drop_index(op.f('ix_email_messages_is_read'), table_name='email_messages')
    op.drop_index(op.f('ix_email_messages_direction'), table_name='email_messages')
    op.drop_index(op.f('ix_email_messages_message_id'), table_name='email_messages')
    op.drop_index(op.f('ix_email_messages_thread_id'), table_name='email_messages')
    op.drop_index(op.f('ix_email_messages_application_id'), table_name='email_messages')
    op.drop_index(op.f('ix_email_messages_email_account_id'), table_name='email_messages')
    op.drop_index(op.f('ix_email_messages_candidate_id'), table_name='email_messages')
    op.drop_index(op.f('ix_email_messages_id'), table_name='email_messages')
    op.drop_table('email_messages')

    op.drop_index('ix_email_threads_latest_message', table_name='email_threads')
    op.drop_index('ix_email_threads_candidate_thread', table_name='email_threads')
    op.drop_index(op.f('ix_email_threads_thread_id'), table_name='email_threads')
    op.drop_index(op.f('ix_email_threads_candidate_id'), table_name='email_threads')
    op.drop_index(op.f('ix_email_threads_id'), table_name='email_threads')
    op.drop_table('email_threads')

    op.drop_index('ix_email_accounts_candidate_email', table_name='email_accounts')
    op.drop_index(op.f('ix_email_accounts_email_address'), table_name='email_accounts')
    op.drop_index(op.f('ix_email_accounts_candidate_id'), table_name='email_accounts')
    op.drop_index(op.f('ix_email_accounts_id'), table_name='email_accounts')
    op.drop_table('email_accounts')
