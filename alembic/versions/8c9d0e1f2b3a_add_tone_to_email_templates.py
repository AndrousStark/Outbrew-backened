"""Add tone to email templates

Revision ID: 8c9d0e1f2b3a
Revises: 7b8c9d0e1f2a
Create Date: 2026-01-16 14:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8c9d0e1f2b3a"
down_revision = "7b8c9d0e1f2a"
branch_labels = None
depends_on = None


def upgrade():
    """Add tone enum and column to email_templates for AI-generated templates"""

    # Check if we're using SQLite
    bind = op.get_bind()
    is_sqlite = bind.engine.url.get_backend_name() == "sqlite"

    # Create enum type for tones (SQLite will use VARCHAR)
    tone_enum = sa.Enum(
        "professional",
        "enthusiastic",
        "story_driven",
        "value_first",
        "consultant",
        "friendly",
        "formal",
        "casual",
        name="emailtone",
    )
    tone_enum.create(bind, checkfirst=True)

    # Add tone column
    op.add_column("email_templates", sa.Column("tone", tone_enum, nullable=True))

    # Add index for faster tone-based queries
    op.create_index("ix_email_templates_tone", "email_templates", ["tone"])

    # Add AI_GENERATED to category enum (PostgreSQL only - SQLite uses VARCHAR)
    if not is_sqlite:
        op.execute("""
            ALTER TYPE templatecategory ADD VALUE IF NOT EXISTS 'ai_generated'
        """)


def downgrade():
    """Remove tone from email_templates"""
    # Drop index
    op.drop_index("ix_email_templates_tone", table_name="email_templates")

    # Drop column
    op.drop_column("email_templates", "tone")

    # Drop enum type
    sa.Enum(name="emailtone").drop(op.get_bind(), checkfirst=True)
