# =============================================================================
# OUTBREW - BACKEND DOCKERFILE
# =============================================================================
# Multi-stage build for optimized production image
# Security-hardened with non-root user
# =============================================================================

# -----------------------------------------------------------------------------
# STAGE 1: Builder
# -----------------------------------------------------------------------------
FROM python:3.11-slim as builder

# Build arguments
ARG BUILD_ENV=production

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install additional production dependencies
RUN pip install --no-cache-dir \
    gunicorn==21.2.0 \
    uvloop==0.19.0 \
    httptools==0.6.1 \
    dnspython==2.4.2 \
    spacy==3.7.2

# Download spaCy model for NLP
RUN python -m spacy download en_core_web_sm

# -----------------------------------------------------------------------------
# STAGE 2: Production
# -----------------------------------------------------------------------------
FROM python:3.11-slim as production

# Labels
LABEL maintainer="Outbrew Team" \
      version="2.0.0" \
      description="Outbrew Backend API" \
      org.opencontainers.image.source="https://github.com/AndrousStark/Outbrew-backened"

# Build arguments
ARG BUILD_ENV=production
ARG APP_USER=appuser
ARG APP_UID=1000
ARG APP_GID=1000

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PATH="/opt/venv/bin:$PATH" \
    APP_HOME=/app \
    ENVIRONMENT=${BUILD_ENV}

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/cache/apt/archives/*

# Create non-root user
RUN groupadd --gid ${APP_GID} ${APP_USER} && \
    useradd --uid ${APP_UID} --gid ${APP_GID} --shell /bin/bash --create-home ${APP_USER}

# Create application directories
RUN mkdir -p ${APP_HOME}/storage ${APP_HOME}/exports ${APP_HOME}/logs ${APP_HOME}/alembic && \
    chown -R ${APP_USER}:${APP_USER} ${APP_HOME}

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Set working directory
WORKDIR ${APP_HOME}

# Copy application code
COPY --chown=${APP_USER}:${APP_USER} ./app ./app
COPY --chown=${APP_USER}:${APP_USER} ./alembic ./alembic
COPY --chown=${APP_USER}:${APP_USER} ./alembic.ini ./alembic.ini

# Create startup script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "=== Outbrew Backend ===" \n\
echo "Environment: ${ENVIRONMENT}" \n\
echo "Starting at: $(date)" \n\
echo "=================================" \n\
\n\
# Run database migrations\n\
if [ "${RUN_MIGRATIONS:-false}" = "true" ]; then\n\
    echo "Running database migrations..."\n\
    alembic upgrade head\n\
fi\n\
\n\
# Start application\n\
echo "Starting Gunicorn server..."\n\
exec gunicorn app.main:app \\\n\
    --bind 0.0.0.0:8000 \\\n\
    --workers ${WORKERS:-4} \\\n\
    --worker-class uvicorn.workers.UvicornWorker \\\n\
    --timeout ${TIMEOUT:-120} \\\n\
    --keep-alive ${KEEP_ALIVE:-5} \\\n\
    --max-requests ${MAX_REQUESTS:-10000} \\\n\
    --max-requests-jitter ${MAX_REQUESTS_JITTER:-1000} \\\n\
    --graceful-timeout ${GRACEFUL_TIMEOUT:-30} \\\n\
    --access-logfile - \\\n\
    --error-logfile - \\\n\
    --capture-output \\\n\
    --enable-stdio-inheritance \\\n\
    --log-level ${LOG_LEVEL:-info}\n\
' > /entrypoint.sh && chmod +x /entrypoint.sh

# Switch to non-root user
USER ${APP_USER}

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
ENTRYPOINT ["/entrypoint.sh"]
