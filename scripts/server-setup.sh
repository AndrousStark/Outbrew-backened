#!/usr/bin/env bash
# =============================================================================
# Outbrew — Fresh Hetzner VPS Setup (Ubuntu 22.04 / 24.04)
# =============================================================================
# Run this ONCE on a fresh VPS to install all prerequisites.
#
# Usage:
#   ssh root@YOUR_VPS_IP
#   curl -sSL https://raw.githubusercontent.com/AndrousStark/Outbrew-backened/main/scripts/server-setup.sh | bash
#
#   OR copy this file and run:
#   chmod +x server-setup.sh && ./server-setup.sh
# =============================================================================

set -euo pipefail

echo "============================================"
echo "  Outbrew — Server Setup"
echo "============================================"

# ─── System Updates ──────────────────────────────────────────────────────────
echo "[1/6] Updating system packages..."
apt-get update -qq && apt-get upgrade -y -qq

# ─── Install Docker ──────────────────────────────────────────────────────────
echo "[2/6] Installing Docker..."
if ! command -v docker &>/dev/null; then
    curl -fsSL https://get.docker.com | bash
    systemctl enable docker
    systemctl start docker
    echo "  Docker installed: $(docker --version)"
else
    echo "  Docker already installed: $(docker --version)"
fi

# ─── Install Docker Compose (plugin) ────────────────────────────────────────
echo "[3/6] Verifying Docker Compose..."
if docker compose version &>/dev/null; then
    echo "  Docker Compose available: $(docker compose version)"
else
    apt-get install -y -qq docker-compose-plugin
    echo "  Docker Compose installed: $(docker compose version)"
fi

# ─── Install essential tools ─────────────────────────────────────────────────
echo "[4/6] Installing tools (git, curl, ufw, fail2ban)..."
apt-get install -y -qq git curl ufw fail2ban htop

# ─── Firewall Setup ─────────────────────────────────────────────────────────
echo "[5/6] Configuring firewall..."
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp    # SSH
ufw allow 80/tcp    # HTTP
ufw allow 443/tcp   # HTTPS
ufw --force enable
echo "  Firewall enabled (SSH, HTTP, HTTPS allowed)"

# ─── Clone & Configure ──────────────────────────────────────────────────────
echo "[6/6] Cloning Outbrew backend..."
APP_DIR="/opt/outbrew"
if [ ! -d "$APP_DIR" ]; then
    git clone https://github.com/AndrousStark/Outbrew-backened.git "$APP_DIR"
    cd "$APP_DIR"
else
    echo "  $APP_DIR already exists, pulling latest..."
    cd "$APP_DIR"
    git pull origin main
fi

# Create .env from template
if [ ! -f ".env" ]; then
    cp .env.production.example .env

    # Auto-generate secrets
    SECRET_KEY=$(openssl rand -hex 32)
    ENCRYPTION_KEY=$(openssl rand -hex 32)
    WEBHOOK_SECRET=$(openssl rand -hex 16)
    DB_PASSWORD=$(openssl rand -hex 16)
    REDIS_PASSWORD=$(openssl rand -hex 16)

    sed -i "s|CHANGE_ME_SECRET_KEY_HERE|${SECRET_KEY}|" .env
    sed -i "s|CHANGE_ME_ENCRYPTION_KEY_HERE|${ENCRYPTION_KEY}|" .env
    sed -i "s|CHANGE_ME_WEBHOOK_SECRET_HERE|${WEBHOOK_SECRET}|" .env
    sed -i "s|CHANGE_ME_STRONG_DB_PASSWORD_HERE|${DB_PASSWORD}|" .env
    sed -i "s|CHANGE_ME_STRONG_REDIS_PASSWORD|${REDIS_PASSWORD}|" .env

    echo "  .env created with auto-generated secrets"
    echo ""
    echo "  IMPORTANT: Edit .env to set your domain and CORS origins:"
    echo "    nano $APP_DIR/.env"
fi

chmod +x scripts/deploy.sh scripts/server-setup.sh

echo ""
echo "============================================"
echo "  Setup Complete!"
echo "============================================"
echo ""
echo "  Next steps:"
echo "    1. Edit .env with your values:"
echo "       nano $APP_DIR/.env"
echo ""
echo "    2. Edit nginx domain config:"
echo "       nano $APP_DIR/nginx/conf.d/default.conf"
echo "       (Replace YOUR_DOMAIN with your actual domain)"
echo ""
echo "    3. Deploy:"
echo "       cd $APP_DIR"
echo "       ./scripts/deploy.sh"
echo ""
echo "    4. After DNS is set, get SSL:"
echo "       ./scripts/deploy.sh ssl YOUR_DOMAIN YOUR_EMAIL"
echo ""
echo "============================================"
