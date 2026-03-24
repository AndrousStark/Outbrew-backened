#!/usr/bin/env bash
# =============================================================================
# Outbrew Backend — VPS Deployment Script (Hetzner cx23 + Neon PostgreSQL)
# =============================================================================
# API container on VPS. Database on Neon. Redis on existing VPS Valkey.
#
# Usage:
#   ./scripts/deploy-vps.sh              # First-time setup
#   ./scripts/deploy-vps.sh update       # Pull latest + restart
#   ./scripts/deploy-vps.sh ssl          # Get SSL certificate
#   ./scripts/deploy-vps.sh logs         # View API logs
#   ./scripts/deploy-vps.sh status       # Check health
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
    [ -f ".env" ] || err ".env not found. Run:\n  cp .env.vps.example .env && nano .env"

    source .env
    [ -z "${SECRET_KEY:-}" ] && err "SECRET_KEY not set in .env"
    [ "${SECRET_KEY}" = "CHANGE_ME_SECRET_KEY_HERE" ] && err "SECRET_KEY is still placeholder. Generate: openssl rand -hex 32"
    [ -z "${DATABASE_URL:-}" ] && err "DATABASE_URL not set in .env. Get it from Neon dashboard."
    echo "${DATABASE_URL}" | grep -q "neon.tech" || warn "DATABASE_URL doesn't contain neon.tech — are you sure it's correct?"

    # Check existing Redis is running
    docker exec unjynx-valkey-1 redis-cli ping >/dev/null 2>&1 || warn "Redis (unjynx-valkey-1) not running — caching will be disabled"

    log "Prerequisites OK"
}

# ─────────────────────────────────────────────────────────────────────────────
full_setup() {
    check_prerequisites

    log "=== First-time Outbrew deployment (Neon PostgreSQL) ==="

    # Create storage dirs
    mkdir -p storage exports

    # Build slim image
    log "Building Outbrew API image (slim, no spaCy)..."
    $COMPOSE build --no-cache api

    # Start API
    log "Starting Outbrew API on port 8001..."
    $COMPOSE up -d api

    # Wait for health
    log "Waiting for API health (tables auto-created on Neon)..."
    for i in $(seq 1 45); do
        if curl -sf http://localhost:8001/health >/dev/null 2>&1; then
            log "API is healthy! Tables created on Neon."
            break
        fi
        [ "$i" -eq 45 ] && warn "Health check timed out (may still be starting)"
        sleep 2
    done

    # Install nginx config
    if [ -f "nginx/outbrew-api.conf" ]; then
        log "Installing Nginx config..."
        if [ -d "/etc/nginx/sites-enabled" ]; then
            cp nginx/outbrew-api.conf /etc/nginx/sites-enabled/outbrew-api.conf
        elif [ -d "/etc/nginx/conf.d" ]; then
            cp nginx/outbrew-api.conf /etc/nginx/conf.d/outbrew-api.conf
        else
            warn "Could not find nginx config dir. Copy manually."
        fi

        if nginx -t 2>/dev/null; then
            systemctl reload nginx
            log "Nginx reloaded with Outbrew config"
        else
            warn "Nginx config test failed — check /etc/nginx/ manually"
        fi
    fi

    SERVER_IP=$(curl -4 -sf ifconfig.me 2>/dev/null || echo "YOUR_IP")

    log "============================================"
    log "Outbrew deployed!"
    info "API:     http://localhost:8001"
    info "Health:  http://localhost:8001/health"
    info "Docs:    http://localhost:8001/api/docs"
    info "DB:      Neon PostgreSQL (serverless)"
    info ""
    info "Next steps:"
    info "  1. Add DNS A record: api.outbrew.metaminds.store -> ${SERVER_IP}"
    info "  2. Run: ./scripts/deploy-vps.sh ssl"
    info "  3. Update Vercel env: NEXT_PUBLIC_BACKEND_HOST=https://api.outbrew.metaminds.store"
    info "  4. Update Vercel env: NEXT_PUBLIC_API_URL=https://api.outbrew.metaminds.store/api/v1"
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
    stop)    log "Stopping..."; $COMPOSE down; log "Stopped" ;;
    restart) log "Restarting..."; $COMPOSE restart; log "Restarted" ;;
    *)       full_setup ;;
esac
