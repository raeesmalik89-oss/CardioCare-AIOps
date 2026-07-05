#!/bin/bash
# CardioCare-AIOps — EC2 Setup Script
# Run once after cloning on a fresh EC2 instance.
# Tested on: Amazon Linux 2023 / Ubuntu 26.04 LTS
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
NEEDS_SESSION_REFRESH=false
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
    NEEDS_SESSION_REFRESH=true
    success "Docker installed."
else
    success "Docker already installed: $(docker --version)"
fi

if ! id -nG "$USER" | grep -qw docker; then
    sudo usermod -aG docker "$USER"
    NEEDS_SESSION_REFRESH=true
    info "Added $USER to the docker group (takes effect after reconnecting)."
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

# Prefer ifconfig.me (simple, works regardless of IMDS version). Fall back to IMDSv2
# (token-based — required on instances that enforce it, e.g. HttpTokens=required).
PUBLIC_IP=$(curl -s --max-time 3 ifconfig.me 2>/dev/null)
if [ -z "$PUBLIC_IP" ]; then
    IMDS_TOKEN=$(curl -s --max-time 3 -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" 2>/dev/null)
    PUBLIC_IP=$(curl -s --max-time 3 -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
        http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null)
fi
# Only accept something that actually looks like an IPv4 address — anything else
# (e.g. an HTML error page from a failed request) would corrupt the sed substitution below.
if ! echo "$PUBLIC_IP" | grep -qE '^[0-9]{1,3}(\.[0-9]{1,3}){3}$'; then
    warn "Could not determine public IP automatically — defaulting to 'localhost'. Set EC2_PUBLIC_IP in .env manually."
    PUBLIC_IP="localhost"
fi
sed -i "s/YOUR_EC2_PUBLIC_IP_HERE/$PUBLIC_IP/" .env
success "EC2 public IP set: $PUBLIC_IP"

if grep -q "REPLACE_WITH_URLSAFE_BASE64_ENCODED_32_BYTE_KEY" .env; then
    EVENT_KEY=$(openssl rand -base64 32 | tr -d '\n')
    sed -i "s|REPLACE_WITH_URLSAFE_BASE64_ENCODED_32_BYTE_KEY|$EVENT_KEY|" .env
    success "Generated AES-256 event encryption key."
fi

# ── 4. Sysctl for Kafka (Elasticsearch/Kafka requirement) ────────────────────
sudo sysctl -w vm.max_map_count=262144 2>/dev/null || true
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf >/dev/null 2>&1 || true

# ── 5. Pre-pull images for faster startup ─────────────────────────────────────
if [ "$NEEDS_SESSION_REFRESH" = true ]; then
    warn "Skipping image pre-pull — the docker group membership just changed and won't"
    warn "take effect until you reconnect (see the notice below)."
else
    info "Pre-pulling Docker images (this takes ~2 minutes)..."
    docker compose pull --quiet 2>/dev/null || warn "Some images couldn't be pulled in advance — they'll pull on startup."
fi

# ── 6. Done ───────────────────────────────────────────────────────────────────
if [ "$NEEDS_SESSION_REFRESH" = true ]; then
    echo ""
    echo "  ╔══════════════════════════════════════════════════════════════╗"
    echo "  ║  IMPORTANT: log out and reconnect (exit + re-SSH) now.       ║"
    echo "  ║  Your user was just added to the docker group — that only   ║"
    echo "  ║  takes effect in a NEW session. Running docker commands in  ║"
    echo "  ║  this shell will fail with 'permission denied' until then.  ║"
    echo "  ╚══════════════════════════════════════════════════════════════╝"
fi
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
echo "  ║    Prometheus: http://$PUBLIC_IP:9091                        ║"
echo "  ║    FastAPI:    http://$PUBLIC_IP:8000/docs                   ║"
echo "  ║    Kafka UI:   http://$PUBLIC_IP:8085                        ║"
echo "  ║    Keycloak:   http://$PUBLIC_IP:8095  (admin/CardioCare@2024)║"
echo "  ║    Jaeger:     http://$PUBLIC_IP:16686                       ║"
echo "  ║    Alert Fn:   http://$PUBLIC_IP:5000/alerts                 ║"
echo "  ║    OPA:        http://$PUBLIC_IP:8181                        ║"
echo "  ║    Loki:       http://$PUBLIC_IP:3100                        ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo ""
warn "NOTE: Open EC2 Security Group ports: 22, 3000, 9091, 8000, 8095, 8085, 8181, 16686, 5000, 3100, 9092, 9308, 8001, 8002"
warn "      (4317/4318 only if an external service will send OTLP traces to Jaeger)"
echo ""
