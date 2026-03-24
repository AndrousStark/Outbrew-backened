#!/usr/bin/env bash
# =============================================================================
# Outbrew Backend — VPS Deployment Script (Hetzner cx23)
# =============================================================================
# Shares existing PostgreSQL, Redis, and Nginx on the VPS.
# Only deploys the Outbrew API container.
#
# Usage:
#   ./scripts/deploy-vps.sh              # First-time setup
#   ./scripts/deploy-vps.sh update       # Pull latest + restart
#   ./scripts/deploy-vps.sh ssl          # Get SSL certificate
#   ./scripts/deploy-vps.sh logs         # View API logs
#   ./scripts/deploy-vps.sh status       # Check health
#   ./scripts/deploy-vps.sh db-setup     # Create outbrew database
# =============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${GREEN}[OUTBREW]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
info() { echo -e "${BLUE}[INFO]${NC} $1"; }

COMPOSE="docker compose -f docker-compose.vps.yml"

# ─────────────────────────────────────────────────────────────────────────────
check_prerequisites() {
    log "Checking prerequisites..."

    command -v docker >/dev/null 2>&1 || err "Docker not installed"
    [ -f ".env" ] || err ".env not found. Create from .env.production.example"

    source .env
    [ -z "${SECRET_KEY:-}" ] && err "SECRET_KEY not set in .env"
    [ "${SECRET_KEY}" = "CHANGE_ME_SECRET_KEY_HERE" ] && err "SECRET_KEY is placeholder"

    # Check existing PostgreSQL is running
    docker exec unjynx-postgres-1 pg_isready >/dev/null 2>&1 || err "PostgreSQL (unjynx-postgres-1) is not running"

    # Check existing Redis is running
    docker exec unjynx-valkey-1 redis-cli ping >/dev/null 2>&1 || err "Redis (unjynx-valkey-1) is not running"

    log "Prerequisites OK"
}

# ─────────────────────────────────────────────────────────────────────────────
db_setup() {
    log "Creating outbrew database in existing PostgreSQL..."

    source .env
    local PG_USER="${POSTGRES_USER:-outbrew_user}"
    local PG_PASS="${POSTGRES_PASSWORD}"
    local PG_DB="${POSTGRES_DB:-outbrew}"

    # Create user (ignore if exists)
    docker exec unjynx-postgres-1 psql -U postgres -c \
        "DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='${PG_USER}') THEN CREATE ROLE ${PG_USER} WITH LOGIN PASSWORD '${PG_PASS}'; END IF; END \$\$;" 2>/dev/null || true

    # Create database (ignore if exists)
    docker exec unjynx-postgres-1 psql -U postgres -c \
        "SELECT 1 FROM pg_database WHERE datname='${PG_DB}'" | grep -q 1 || \
        docker exec unjynx-postgres-1 psql -U postgres -c "CREATE DATABASE ${PG_DB} OWNER ${PG_USER};"

    # Grant privileges
    docker exec unjynx-postgres-1 psql -U postgres -c \
        "GRANT ALL PRIVILEGES ON DATABASE ${PG_DB} TO ${PG_USER};"

    log "Database '${PG_DB}' ready (user: ${PG_USER})"
}

# ─────────────────────────────────────────────────────────────────────────────
full_setup() {
    check_prerequisites

    log "=== First-time Outbrew deployment ==="

    # Create database
    db_setup

    # Create storage dirs
    mkdir -p storage exports

    # Build slim image
    log "Building Outbrew API image (slim, no spaCy)..."
    $COMPOSE build --no-cache api

    # Start API
    log "Starting Outbrew API on port 8001..."
    $COMPOSE up -d api

    # Wait for health
    log "Waiting for API health..."
    for i in $(seq 1 30); do
        if curl -sf http://localhost:8001/health >/dev/null 2>&1; then
            log "API is healthy!"
            break
        fi
        [ "$i" -eq 30 ] && warn "Health check timed out (may still be starting)"
        sleep 2
    done

    # Install nginx config
    if [ -f "nginx/outbrew-api.conf" ]; then
        log "Installing Nginx config..."
        cp nginx/outbrew-api.conf /etc/nginx/sites-enabled/outbrew-api.conf 2>/dev/null || \
            cp nginx/outbrew-api.conf /etc/nginx/conf.d/outbrew-api.conf 2>/dev/null || \
            warn "Could not auto-install nginx config. Copy manually."

        if nginx -t 2>/dev/null; then
            systemctl reload nginx
            log "Nginx reloaded with Outbrew config"
        else
            warn "Nginx config test failed. Check /etc/nginx/ manually."
        fi
    fi

    log "============================================"
    log "Outbrew deployed!"
    info "API:     http://localhost:8001"
    info "Health:  http://localhost:8001/health"
    info "Docs:    http://localhost:8001/api/docs"
    info ""
    info "Next: Point api.outbrew.metaminds.store DNS → $(curl -4 -s ifconfig.me)"
    info "Then: ./scripts/deploy-vps.sh ssl"
    log "============================================"
}

# ─────────────────────────────────────────────────────────────────────────────
update() {
    log "Pulling latest code..."
    git pull origin main

    log "Rebuilding API..."
    $COMPOSE build api

    log "Restarting..."
    $COMPOSE up -d --no-deps api

    for i in $(seq 1 20); do
        if curl -sf http://localhost:8001/health >/dev/null 2>&1; then
            log "API healthy after update"
            break
        fi
        [ "$i" -eq 20 ] && warn "Health check timed out"
        sleep 2
    done

    log "Update complete!"
}

# ─────────────────────────────────────────────────────────────────────────────
obtain_ssl() {
    local DOMAIN="api.outbrew.metaminds.store"
    local EMAIL="${1:-aniruddh.atrey111101@gmail.com}"

    log "Obtaining SSL for ${DOMAIN}..."
    certbot --nginx -d "${DOMAIN}" --email "${EMAIL}" --agree-tos --no-eff-email

    log "SSL certificate installed!"
}

# ─────────────────────────────────────────────────────────────────────────────
view_logs() {
    $COMPOSE logs -f --tail=100 api
}

# ─────────────────────────────────────────────────────────────────────────────
status() {
    log "Outbrew API status:"
    $COMPOSE ps

    echo ""
    if curl -sf http://localhost:8001/health 2>/dev/null | python3 -m json.tool 2>/dev/null; then
        echo ""
    else
        warn "API health check failed"
    fi

    info "Memory usage:"
    docker stats outbrew-api --no-stream --format "  CPU: {{.CPUPerc}}  MEM: {{.MemUsage}}" 2>/dev/null || warn "Container not running"
}

# ─────────────────────────────────────────────────────────────────────────────
case "${1:-}" in
    update)  update ;;
    ssl)     obtain_ssl "${2:-}" ;;
    logs)    view_logs ;;
    status)  status ;;
    db-setup) source .env; db_setup ;;
    stop)    log "Stopping..."; $COMPOSE down; log "Stopped" ;;
    restart) log "Restarting..."; $COMPOSE restart; log "Restarted" ;;
    *)       full_setup ;;
esac
