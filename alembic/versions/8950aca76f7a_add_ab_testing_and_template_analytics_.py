"""add ab testing and template analytics tables

Revision ID: 8950aca76f7a
Revises: 37725b57f1c8
Create Date: 2026-01-12 15:25:28.183668

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8950aca76f7a'
down_revision: Union[str, Sequence[str], None] = '37725b57f1c8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - Add A/B testing and template analytics tables."""

    # ========================================
    # A/B Testing Tables
    # ========================================

    # 1. Create ab_tests table (without winner FK - circular reference issue)
    op.create_table(
        'ab_tests',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=200), nullable=False),
        sa.Column('description', sa.Text()),
        sa.Column('hypothesis', sa.Text()),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='draft'),
        sa.Column('allocation_strategy', sa.String(length=50), server_default='even'),
        sa.Column('minimum_sample_size', sa.Integer(), server_default='30'),
        sa.Column('confidence_level', sa.Float(), server_default='0.95'),
        sa.Column('primary_metric', sa.String(length=50), server_default='reply_rate'),
        sa.Column('secondary_metrics', sa.JSON(), server_default='[]'),
        sa.Column('winner_variant_id', sa.Integer()),  # No FK constraint due to circular reference
        sa.Column('winner_selected_at', sa.DateTime(timezone=True)),
        sa.Column('winner_selection_method', sa.String(length=50)),
        sa.Column('total_campaigns', sa.Integer(), server_default='0'),
        sa.Column('statistical_significance', sa.Float()),
        sa.Column('confidence_interval', sa.JSON(), server_default='{}'),
        sa.Column('started_at', sa.DateTime(timezone=True)),
        sa.Column('completed_at', sa.DateTime(timezone=True)),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), onupdate=sa.func.now()),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_ab_tests_candidate', 'ab_tests', ['candidate_id'])
    op.create_index('ix_ab_tests_status', 'ab_tests', ['status'])

    # 2. Create ab_test_variants table
    op.create_table(
        'ab_test_variants',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('test_id', sa.Integer(), nullable=False),
        sa.Column('sequence_id', sa.Integer()),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('description', sa.Text()),
        sa.Column('is_control', sa.Boolean(), server_default='false'),
        sa.Column('allocation_weight', sa.Float(), server_default='1.0'),
        sa.Column('total_campaigns', sa.Integer(), server_default='0'),
        sa.Column('total_emails_sent', sa.Integer(), server_default='0'),
        sa.Column('total_opens', sa.Integer(), server_default='0'),
        sa.Column('total_clicks', sa.Integer(), server_default='0'),
        sa.Column('total_replies', sa.Integer(), server_default='0'),
        sa.Column('open_rate', sa.Float(), server_default='0.0'),
        sa.Column('click_rate', sa.Float(), server_default='0.0'),
        sa.Column('reply_rate', sa.Float(), server_default='0.0'),
        sa.Column('avg_time_to_reply_hours', sa.Float()),
        sa.Column('avg_emails_to_reply', sa.Float()),
        sa.Column('conversion_rate', sa.Float(), server_default='0.0'),
        sa.Column('std_error', sa.Float()),
        sa.Column('confidence_interval_lower', sa.Float()),
        sa.Column('confidence_interval_upper', sa.Float()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), onupdate=sa.func.now()),
        sa.ForeignKeyConstraint(['test_id'], ['ab_tests.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['sequence_id'], ['follow_up_sequences.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_ab_test_variants_test', 'ab_test_variants', ['test_id'])
    op.create_index('ix_ab_test_variants_sequence', 'ab_test_variants', ['sequence_id'])

    # 3. Create ab_test_assignments table
    op.create_table(
        'ab_test_assignments',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('test_id', sa.Integer(), nullable=False),
        sa.Column('variant_id', sa.Integer(), nullable=False),
        sa.Column('campaign_id', sa.Integer(), nullable=False),
        sa.Column('assigned_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('assignment_method', sa.String(length=50)),
        sa.Column('has_reply', sa.Boolean(), server_default='false'),
        sa.Column('reply_received_at', sa.DateTime(timezone=True)),
        sa.Column('emails_sent', sa.Integer(), server_default='0'),
        sa.Column('emails_opened', sa.Integer(), server_default='0'),
        sa.Column('time_to_reply_hours', sa.Float()),
        sa.ForeignKeyConstraint(['test_id'], ['ab_tests.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['variant_id'], ['ab_test_variants.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['campaign_id'], ['follow_up_campaigns.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('campaign_id', name='unique_campaign_assignment')
    )
    op.create_index('ix_ab_test_assignment_test', 'ab_test_assignments', ['test_id'])
    op.create_index('ix_ab_test_assignment_variant', 'ab_test_assignments', ['variant_id'])
    op.create_index('ix_ab_test_assignment_campaign', 'ab_test_assignments', ['campaign_id'])
    op.create_index('ix_ab_test_assignment_test_variant', 'ab_test_assignments', ['test_id', 'variant_id'])

    # ========================================
    # Template Analytics Tables
    # ========================================

    # 4. Create template_analytics_events table
    op.create_table(
        'template_analytics_events',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('template_id', sa.Integer(), nullable=False),
        sa.Column('template_version_id', sa.Integer()),
        sa.Column('user_id', sa.Integer()),
        sa.Column('event_type', sa.String(length=50), nullable=False),
        sa.Column('event_metadata', sa.JSON(), server_default='{}'),
        sa.Column('session_id', sa.String(length=100)),
        sa.Column('referrer', sa.String(length=500)),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['template_id'], ['public_templates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['template_version_id'], ['template_versions.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['user_id'], ['candidates.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_template_analytics_template', 'template_analytics_events', ['template_id'])
    op.create_index('ix_template_analytics_user', 'template_analytics_events', ['user_id'])
    op.create_index('ix_template_analytics_event_type', 'template_analytics_events', ['event_type'])
    op.create_index('ix_template_analytics_created', 'template_analytics_events', ['created_at'])
    op.create_index('ix_template_analytics_template_event', 'template_analytics_events', ['template_id', 'event_type'])
    op.create_index('ix_template_analytics_user_event', 'template_analytics_events', ['user_id', 'event_type'])

    # 5. Create template_performance_snapshots table
    op.create_table(
        'template_performance_snapshots',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('template_id', sa.Integer(), nullable=False),
        sa.Column('template_version_id', sa.Integer()),
        sa.Column('snapshot_date', sa.DateTime(timezone=True), nullable=False),
        sa.Column('period_type', sa.String(length=20), nullable=False),
        sa.Column('period_start', sa.DateTime(timezone=True), nullable=False),
        sa.Column('period_end', sa.DateTime(timezone=True), nullable=False),
        sa.Column('total_views', sa.Integer(), server_default='0'),
        sa.Column('unique_viewers', sa.Integer(), server_default='0'),
        sa.Column('total_clones', sa.Integer(), server_default='0'),
        sa.Column('total_uses', sa.Integer(), server_default='0'),
        sa.Column('total_favorites', sa.Integer(), server_default='0'),
        sa.Column('new_ratings', sa.Integer(), server_default='0'),
        sa.Column('new_reviews', sa.Integer(), server_default='0'),
        sa.Column('avg_rating_period', sa.Float()),
        sa.Column('cumulative_avg_rating', sa.Float()),
        sa.Column('emails_sent', sa.Integer(), server_default='0'),
        sa.Column('emails_opened', sa.Integer(), server_default='0'),
        sa.Column('emails_replied', sa.Integer(), server_default='0'),
        sa.Column('open_rate', sa.Float()),
        sa.Column('reply_rate', sa.Float()),
        sa.Column('view_to_clone_rate', sa.Float()),
        sa.Column('clone_to_use_rate', sa.Float()),
        sa.Column('view_to_favorite_rate', sa.Float()),
        sa.Column('views_growth_pct', sa.Float()),
        sa.Column('uses_growth_pct', sa.Float()),
        sa.Column('rating_growth_pct', sa.Float()),
        sa.Column('rank_in_category', sa.Integer()),
        sa.Column('rank_overall', sa.Integer()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['template_id'], ['public_templates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['template_version_id'], ['template_versions.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('template_id', 'period_type', 'snapshot_date', name='unique_template_period_snapshot')
    )
    op.create_index('ix_template_snapshot_template', 'template_performance_snapshots', ['template_id'])
    op.create_index('ix_template_snapshot_date', 'template_performance_snapshots', ['snapshot_date'])
    op.create_index('ix_template_snapshot_period', 'template_performance_snapshots', ['period_type', 'snapshot_date'])
    op.create_index('ix_template_snapshot_template_date', 'template_performance_snapshots', ['template_id', 'snapshot_date'])

    # 6. Create template_ab_test_results table
    op.create_table(
        'template_ab_test_results',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('template_id', sa.Integer(), nullable=False),
        sa.Column('test_name', sa.String(length=200), nullable=False),
        sa.Column('test_description', sa.Text()),
        sa.Column('test_hypothesis', sa.Text()),
        sa.Column('variant_a_version_id', sa.Integer()),
        sa.Column('variant_b_version_id', sa.Integer()),
        sa.Column('test_dimension', sa.String(length=50)),
        sa.Column('variant_a_uses', sa.Integer(), server_default='0'),
        sa.Column('variant_b_uses', sa.Integer(), server_default='0'),
        sa.Column('variant_a_reply_rate', sa.Float()),
        sa.Column('variant_b_reply_rate', sa.Float()),
        sa.Column('variant_a_open_rate', sa.Float()),
        sa.Column('variant_b_open_rate', sa.Float()),
        sa.Column('p_value', sa.Float()),
        sa.Column('confidence_level', sa.Float(), server_default='0.95'),
        sa.Column('is_significant', sa.Boolean(), server_default='false'),
        sa.Column('winner', sa.String(length=10)),
        sa.Column('relative_improvement', sa.Float()),
        sa.Column('absolute_difference', sa.Float()),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True)),
        sa.Column('status', sa.String(length=20), server_default='running'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['template_id'], ['public_templates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['variant_a_version_id'], ['template_versions.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['variant_b_version_id'], ['template_versions.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_template_abtest_template', 'template_ab_test_results', ['template_id'])
    op.create_index('ix_template_abtest_status', 'template_ab_test_results', ['status'])
    op.create_index('ix_template_abtest_completed', 'template_ab_test_results', ['completed_at'])
    op.create_index('ix_template_abtest_template_status', 'template_ab_test_results', ['template_id', 'status'])


def downgrade() -> None:
    """Downgrade schema - Remove A/B testing and template analytics tables."""

    # Drop in reverse order to handle foreign key dependencies

    # Template analytics tables
    op.drop_index('ix_template_abtest_template_status', table_name='template_ab_test_results')
    op.drop_index('ix_template_abtest_completed', table_name='template_ab_test_results')
    op.drop_index('ix_template_abtest_status', table_name='template_ab_test_results')
    op.drop_index('ix_template_abtest_template', table_name='template_ab_test_results')
    op.drop_table('template_ab_test_results')

    op.drop_index('ix_template_snapshot_template_date', table_name='template_performance_snapshots')
    op.drop_index('ix_template_snapshot_period', table_name='template_performance_snapshots')
    op.drop_index('ix_template_snapshot_date', table_name='template_performance_snapshots')
    op.drop_index('ix_template_snapshot_template', table_name='template_performance_snapshots')
    op.drop_table('template_performance_snapshots')

    op.drop_index('ix_template_analytics_user_event', table_name='template_analytics_events')
    op.drop_index('ix_template_analytics_template_event', table_name='template_analytics_events')
    op.drop_index('ix_template_analytics_created', table_name='template_analytics_events')
    op.drop_index('ix_template_analytics_event_type', table_name='template_analytics_events')
    op.drop_index('ix_template_analytics_user', table_name='template_analytics_events')
    op.drop_index('ix_template_analytics_template', table_name='template_analytics_events')
    op.drop_table('template_analytics_events')

    # A/B testing tables
    op.drop_index('ix_ab_test_assignment_test_variant', table_name='ab_test_assignments')
    op.drop_index('ix_ab_test_assignment_campaign', table_name='ab_test_assignments')
    op.drop_index('ix_ab_test_assignment_variant', table_name='ab_test_assignments')
    op.drop_index('ix_ab_test_assignment_test', table_name='ab_test_assignments')
    op.drop_table('ab_test_assignments')

    op.drop_index('ix_ab_test_variants_sequence', table_name='ab_test_variants')
    op.drop_index('ix_ab_test_variants_test', table_name='ab_test_variants')
    op.drop_table('ab_test_variants')

    op.drop_index('ix_ab_tests_status', table_name='ab_tests')
    op.drop_index('ix_ab_tests_candidate', table_name='ab_tests')
    op.drop_table('ab_tests')
