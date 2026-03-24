#!/usr/bin/env bash
# =============================================================================
# Outbrew Backend — Production Deployment Script
# =============================================================================
# Usage (on your Hetzner VPS):
#   chmod +x scripts/deploy.sh
#   ./scripts/deploy.sh           # Full first-time setup
#   ./scripts/deploy.sh update    # Pull latest code and restart
#   ./scripts/deploy.sh ssl       # Obtain SSL certificate
#   ./scripts/deploy.sh backup    # Backup database
#   ./scripts/deploy.sh logs      # View live logs
#   ./scripts/deploy.sh status    # Check service status
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${GREEN}[DEPLOY]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
info() { echo -e "${BLUE}[INFO]${NC} $1"; }

COMPOSE="docker compose -f docker-compose.prod.yml"
BACKUP_DIR="./backups"

# ─────────────────────────────────────────────────────────────────────────────
# Prerequisites check
# ─────────────────────────────────────────────────────────────────────────────
check_prerequisites() {
    log "Checking prerequisites..."

    command -v docker >/dev/null 2>&1 || err "Docker is not installed. Install: https://docs.docker.com/engine/install/ubuntu/"
    command -v docker compose >/dev/null 2>&1 || command -v docker-compose >/dev/null 2>&1 || err "Docker Compose is not installed."

    if [ ! -f ".env" ]; then
        err ".env file not found. Copy and edit .env.production.example:\n  cp .env.production.example .env\n  nano .env"
    fi

    # Validate critical env vars
    source .env
    [ -z "${SECRET_KEY:-}" ] && err "SECRET_KEY is not set in .env"
    [ "${SECRET_KEY}" = "CHANGE_ME_SECRET_KEY_HERE" ] && err "SECRET_KEY is still the placeholder. Generate with: openssl rand -hex 32"
    [ -z "${POSTGRES_PASSWORD:-}" ] && err "POSTGRES_PASSWORD is not set in .env"
    [ "${POSTGRES_PASSWORD}" = "CHANGE_ME_STRONG_DB_PASSWORD_HERE" ] && err "POSTGRES_PASSWORD is still the placeholder."
    grep -q "YOUR_DOMAIN" nginx/conf.d/default.conf 2>/dev/null && err "Replace YOUR_DOMAIN in nginx/conf.d/default.conf with your actual domain before deploying."

    log "Prerequisites OK"
}

# ─────────────────────────────────────────────────────────────────────────────
# First-time setup
# ─────────────────────────────────────────────────────────────────────────────
full_setup() {
    check_prerequisites
    log "Starting full deployment..."

    # Create required directories
    mkdir -p nginx/conf.d backups

    # Build and start services
    log "Building Docker images..."
    $COMPOSE build --no-cache api

    log "Starting services..."
    $COMPOSE up -d postgres redis
    sleep 5

    # Wait for PostgreSQL
    log "Waiting for PostgreSQL to be ready..."
    for i in $(seq 1 30); do
        if $COMPOSE exec postgres pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
            log "PostgreSQL is ready"
            break
        fi
        [ "$i" -eq 30 ] && err "PostgreSQL failed to start in 30s"
        sleep 1
    done

    # Start API
    log "Starting API..."
    $COMPOSE up -d api

    # Wait for API health
    log "Waiting for API to be healthy..."
    for i in $(seq 1 60); do
        if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
            log "API is healthy"
            break
        fi
        [ "$i" -eq 60 ] && warn "API health check timed out (may still be starting)"
        sleep 2
    done

    # Start Nginx
    log "Starting Nginx..."
    $COMPOSE up -d nginx

    log "============================================"
    log "Deployment complete!"
    info "API:    http://$(hostname -I | awk '{print $1}'):8000"
    info "Nginx:  http://$(hostname -I | awk '{print $1}')"
    info "Docs:   http://$(hostname -I | awk '{print $1}')/api/docs"
    info ""
    info "Next steps:"
    info "  1. Point your domain DNS A record to this server's IP"
    info "  2. Edit nginx/conf.d/default.conf — replace YOUR_DOMAIN"
    info "  3. Run: ./scripts/deploy.sh ssl"
    log "============================================"
}

# ─────────────────────────────────────────────────────────────────────────────
# Update deployment (pull latest code)
# ─────────────────────────────────────────────────────────────────────────────
update() {
    log "Pulling latest code..."
    git pull origin main

    log "Backing up database before update..."
    backup

    log "Rebuilding API image..."
    $COMPOSE build api

    log "Restarting API with zero-downtime..."
    $COMPOSE up -d --no-deps api

    # Wait for health
    for i in $(seq 1 30); do
        if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
            log "API is healthy after update"
            break
        fi
        [ "$i" -eq 30 ] && warn "Health check timed out"
        sleep 2
    done

    log "Update complete!"
}

# ─────────────────────────────────────────────────────────────────────────────
# Obtain SSL certificate
# ─────────────────────────────────────────────────────────────────────────────
obtain_ssl() {
    if [ -z "${1:-}" ]; then
        err "Usage: ./scripts/deploy.sh ssl YOUR_DOMAIN YOUR_EMAIL\n  Example: ./scripts/deploy.sh ssl api.outbrew.metaminds.store admin@metaminds.store"
    fi

    DOMAIN="${1}"
    EMAIL="${2:-admin@metaminds.store}"

    log "Obtaining SSL certificate for ${DOMAIN}..."

    # Ensure nginx is running for ACME challenge
    $COMPOSE up -d nginx

    $COMPOSE run --rm certbot certonly \
        --webroot \
        -w /var/www/certbot \
        -d "${DOMAIN}" \
        --email "${EMAIL}" \
        --agree-tos \
        --no-eff-email \
        --force-renewal

    log "SSL certificate obtained!"
    info "Now edit nginx/conf.d/default.conf:"
    info "  1. Replace all YOUR_DOMAIN with ${DOMAIN}"
    info "  2. Uncomment the HTTPS server block"
    info "  3. Uncomment 'return 301' in the HTTP block"
    info "  4. Remove the HTTP location blocks (they move to HTTPS)"
    info "  5. Restart nginx: docker compose -f docker-compose.prod.yml restart nginx"

    # Start certbot auto-renewal
    $COMPOSE up -d certbot
    log "Certbot auto-renewal started (checks every 12 hours)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Database backup
# ─────────────────────────────────────────────────────────────────────────────
backup() {
    mkdir -p "${BACKUP_DIR}"
    source .env

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    BACKUP_FILE="${BACKUP_DIR}/outbrew_db_${TIMESTAMP}.sql.gz"

    log "Backing up database to ${BACKUP_FILE}..."

    $COMPOSE exec -T postgres pg_dump \
        -U "${POSTGRES_USER}" \
        -d "${POSTGRES_DB}" \
        --no-owner \
        --no-privileges \
        --clean \
        --if-exists \
        | gzip > "${BACKUP_FILE}"

    SIZE=$(du -h "${BACKUP_FILE}" | cut -f1)
    log "Backup complete: ${BACKUP_FILE} (${SIZE})"

    # Keep only last 7 backups
    ls -t "${BACKUP_DIR}"/outbrew_db_*.sql.gz 2>/dev/null | tail -n +8 | xargs -r rm
    info "Kept last 7 backups, older ones removed"
}

# ─────────────────────────────────────────────────────────────────────────────
# View logs
# ─────────────────────────────────────────────────────────────────────────────
view_logs() {
    SERVICE="${1:-api}"
    log "Tailing logs for ${SERVICE}..."
    $COMPOSE logs -f --tail=100 "${SERVICE}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Service status
# ─────────────────────────────────────────────────────────────────────────────
status() {
    [ -f ".env" ] && source .env
    log "Service status:"
    $COMPOSE ps

    echo ""
    info "Health checks:"

    # API health
    if curl -sf http://localhost:8000/health 2>/dev/null | python3 -m json.tool 2>/dev/null; then
        echo ""
    else
        warn "API health check failed"
    fi

    # PostgreSQL
    if $COMPOSE exec -T postgres pg_isready -U "${POSTGRES_USER:-outbrew_user}" >/dev/null 2>&1; then
        info "PostgreSQL: READY"
    else
        warn "PostgreSQL: NOT READY"
    fi

    # Redis
    if $COMPOSE exec -T redis redis-cli ping >/dev/null 2>&1; then
        info "Redis: PONG"
    else
        warn "Redis: NOT RESPONDING"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
case "${1:-}" in
    update)
        update
        ;;
    ssl)
        obtain_ssl "${2:-}" "${3:-}"
        ;;
    backup)
        backup
        ;;
    logs)
        view_logs "${2:-api}"
        ;;
    status)
        status
        ;;
    stop)
        log "Stopping all services..."
        $COMPOSE down
        log "All services stopped"
        ;;
    restart)
        log "Restarting all services..."
        $COMPOSE restart
        log "All services restarted"
        ;;
    *)
        full_setup
        ;;
esac
