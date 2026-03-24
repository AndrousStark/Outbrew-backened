from logging.config import fileConfig
import sys
from pathlib import Path

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import our models and database configuration
from app.core.database import Base
from app.core.config import settings

# Import all models so Alembic can detect them
from app.models.candidate import Candidate
from app.models.company import Company
from app.models.application import Application
from app.models.email_log import EmailLog
from app.models.resume import ResumeVersion
from app.models.email_template import EmailTemplate

# Previously missing models - now registered
from app.models.application_history import (
    ApplicationHistory,
    ApplicationNote,
    ApplicationAttachment,
)
from app.models.email_warming import EmailWarmingConfig, EmailWarmingDailyLog
from app.models.rate_limiting import RateLimitConfig, RateLimitUsageLog

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Set the sqlalchemy URL from our settings
if settings.DATABASE_URL:
    config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)
elif settings.POSTGRES_SERVER and settings.POSTGRES_SERVER != "localhost":
    config.set_main_option("sqlalchemy.url", settings.database_url)
else:
    config.set_main_option("sqlalchemy.url", "sqlite:///./outbrew.db")

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        # SQLite needs batch mode to emulate ALTER TABLE when adding FKs/indexes
        is_sqlite = connection.engine.url.get_backend_name() == "sqlite"
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=is_sqlite,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
