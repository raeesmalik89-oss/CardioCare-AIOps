#!/bin/bash
# CardioCare-AIOps — EC2 Setup Script
# Run once after cloning on a fresh EC2 instance.
# Tested on: Amazon Linux 2023 / Ubuntu 22.04
# Recommended: t3.large (8 GB RAM) or t3.medium (4 GB minimum)

set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }

echo ""
echo "  ╔═══════════════════════════════════════════╗"
echo "  ║      CardioCare-AIOps — Setup Script      ║"
echo "  ║   Event-Driven Serverless AIOps Platform  ║"
echo "  ╚═══════════════════════════════════════════╝"
echo ""

# ── 1. Detect OS & Install Docker ────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    info "Installing Docker..."
    if [ -f /etc/amazon-linux-release ] || grep -q "Amazon Linux" /etc/os-release 2>/dev/null; then
        sudo yum update -y
        sudo yum install -y docker
        sudo systemctl start docker
        sudo systemctl enable docker
    else
        curl -fsSL https://get.docker.com | sudo sh
        sudo systemctl start docker
        sudo systemctl enable docker
    fi
    sudo usermod -aG docker "$USER"
    success "Docker installed."
else
    success "Docker already installed: $(docker --version)"
fi

# ── 2. Install Docker Compose v2 ─────────────────────────────────────────────
if ! docker compose version &>/dev/null; then
    info "Installing Docker Compose v2..."
    sudo mkdir -p /usr/local/lib/docker/cli-plugins
    sudo curl -SL "https://github.com/docker/compose/releases/download/v2.23.0/docker-compose-linux-x86_64" \
         -o /usr/local/lib/docker/cli-plugins/docker-compose
    sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
    success "Docker Compose installed."
else
    success "Docker Compose already installed: $(docker compose version)"
fi

# ── 3. Set EC2 public IP in .env ─────────────────────────────────────────────
if [ ! -f .env ]; then
    cp .env.example .env
    info "Created .env from .env.example"
fi

PUBLIC_IP=$(curl -s --max-time 3 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "localhost")
sed -i "s/YOUR_EC2_PUBLIC_IP_HERE/$PUBLIC_IP/" .env
success "EC2 public IP set: $PUBLIC_IP"

# ── 4. Sysctl for Kafka (Elasticsearch/Kafka requirement) ────────────────────
sudo sysctl -w vm.max_map_count=262144 2>/dev/null || true
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf >/dev/null 2>&1 || true

# ── 5. Pre-pull images for faster startup ─────────────────────────────────────
info "Pre-pulling Docker images (this takes ~2 minutes)..."
docker compose pull --quiet 2>/dev/null || warn "Some images couldn't be pulled in advance — they'll pull on startup."

# ── 6. Done ───────────────────────────────────────────────────────────────────
echo ""
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║  Setup complete! Run the platform with:                     ║"
echo "  ║                                                              ║"
echo "  ║    docker compose up -d                                      ║"
echo "  ║    docker compose ps          # check service health        ║"
echo "  ║    docker compose logs -f     # stream all logs             ║"
echo "  ╠══════════════════════════════════════════════════════════════╣"
echo "  ║  Access URLs (replace EC2_IP):                               ║"
echo "  ║    Grafana:    http://$PUBLIC_IP:3000  (admin/CardioCare@2024) ║"
echo "  ║    Prometheus: http://$PUBLIC_IP:9090                        ║"
echo "  ║    FastAPI:    http://$PUBLIC_IP:8000/docs                   ║"
echo "  ║    Kafka UI:   http://$PUBLIC_IP:8085                        ║"
echo "  ║    Keycloak:   http://$PUBLIC_IP:8080  (admin/CardioCare@2024)║"
echo "  ║    Jaeger:     http://$PUBLIC_IP:16686                       ║"
echo "  ║    Alert Fn:   http://$PUBLIC_IP:5000/alerts                 ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo ""
warn "NOTE: Open EC2 Security Group ports: 3000, 9090, 8000, 8080, 8085, 16686, 5000"
echo ""
