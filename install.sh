#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
LOCAL_DIR="$ROOT_DIR/.aniu/local"
ENV_FILE="$LOCAL_DIR/.env"
VENV_DIR="$LOCAL_DIR/.venv"
MODE="docker"
START_SOURCE=0
INSTALL_SYSTEMD=0
SYSTEMD_DIR="${ANIU_SYSTEMD_DIR:-/etc/systemd/system}"
IMAGE="${ANIU_IMAGE:-}"

usage() {
  cat <<'EOF'
Usage: ./install.sh [docker|source|clean] [options]

Modes:
  docker                 Build and start the local Docker image (default).
  source                 Install Python/Node dependencies and build the frontend.
  clean                  Remove rebuildable local caches, logs, and reference artifacts.

Options:
  --image IMAGE          Pull and start a pre-built image instead of building.
  --start                Start backend.serve after a source installation.
  --systemd              Install and start an aniu.service systemd unit (source only).
  --no-start             Do not start after a source installation (default).
  -h, --help             Show this help.

Examples:
  ./install.sh
  ./install.sh docker --image ghcr.io/OWNER/IMAGE:latest
  ./install.sh source --start
  ANIU_SYSTEMD_LAN=1 ANIU_SYSTEMD_ALLOWED_HOSTS=192.168.1.20 ./install.sh source --systemd
  ./install.sh clean
EOF
}

die() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

run_privileged() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  else
    require_command sudo
    sudo "$@"
  fi
}

ensure_local_dir() {
  mkdir -p "$LOCAL_DIR"
  chmod 700 "$ROOT_DIR/.aniu" "$LOCAL_DIR"
}

detect_lan_ipv4() {
  local detected=""
  if command -v python3 >/dev/null 2>&1; then
    detected="$(python3 - <<'PY'
import ipaddress
import socket

try:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
        connection.connect(("8.8.8.8", 80))
        address = ipaddress.IPv4Address(connection.getsockname()[0])
except OSError:
    raise SystemExit

if not (
    address.is_loopback
    or address.is_unspecified
    or address.is_multicast
    or address.is_global
):
    print(address)
PY
)"
  fi
  if [ -z "$detected" ] && command -v hostname >/dev/null 2>&1; then
    detected="$(hostname -I 2>/dev/null | awk '{for (i = 1; i <= NF; i++) if ($i ~ /^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.)/) {print $i; exit}}' || true)"
  fi
  [ -n "$detected" ] || return 1
  printf '%s' "$detected"
}

ensure_env_file() {
  ensure_local_dir
  if [ ! -f "$ENV_FILE" ]; then
    [ -f "$ROOT_DIR/.env.example" ] || die "Missing .env.example"
    cp "$ROOT_DIR/.env.example" "$ENV_FILE"
    printf 'Created local environment file: %s\n' "$ENV_FILE"
  fi
  chmod 600 "$ENV_FILE"
}

check_python_version() {
  python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)' \
    || die "Python 3.12 or newer is required"
}

port_from_env() {
  local port
  port="$(awk -F= '/^[[:space:]]*ANIU_PORT[[:space:]]*=/{value=$2} END{print value}' "$ENV_FILE")"
  printf '%s' "${port:-8000}"
}

clean_local_artifacts() {
  if ps -eo args= | grep -F "$ROOT_DIR/" | grep -v -E 'grep|run_code' >/dev/null; then
    die "Stop Aniu processes before cleaning local artifacts"
  fi

  rm -f -- \
    "$ROOT_DIR/.aniu/aniubot.db" \
    "$ROOT_DIR/.aniu/aniu.sqlite3-wal" \
    "$ROOT_DIR/.aniu/aniu.sqlite3-shm"
  rm -rf -- \
    "$ROOT_DIR/.aniu/skills" \
    "$ROOT_DIR/.aniu/local/.mypy_cache" \
    "$ROOT_DIR/.aniu/local/.pytest_cache" \
    "$ROOT_DIR/.aniu/local/.ruff_cache" \
    "$ROOT_DIR/.aniu/local/pycache" \
    "$ROOT_DIR/.aniu/local/frontend-vite" \
    "$ROOT_DIR/.aniu/local/tsbuildinfo" \
    "$ROOT_DIR/.aniu/local/npm-cache" \
    "$ROOT_DIR/.aniu/local/pip-cache" \
    "$ROOT_DIR/.aniu/local/aniubot.egg-info" \
    "$ROOT_DIR/.aniu/local/build" \
    "$ROOT_DIR/.aniu/local/tools/tdx_probe" \
    "$ROOT_DIR/scripts/tdx_probe" \
    "$ROOT_DIR/frontend/node_modules" \
    "$ROOT_DIR/.aniu/docs/references/dsh-stock-market/node_modules" \
    "$ROOT_DIR/.aniu/docs/references/dsh-stock-market/lib"

  shopt -s globstar nullglob
  local pycache_dirs=("$ROOT_DIR"/backend/**/__pycache__ "$ROOT_DIR"/scripts/**/__pycache__)
  if [ "${#pycache_dirs[@]}" -gt 0 ]; then
    rm -rf -- "${pycache_dirs[@]}"
  fi
  rm -rf -- "$ROOT_DIR/__pycache__" "$ROOT_DIR/frontend/.vite" "$ROOT_DIR/frontend/node_modules/.tmp"
  rm -f -- "$ROOT_DIR/.aniu/logs"/*
  printf 'Rebuildable local artifacts cleaned. Database, key, env, venv, reference source, and private files were kept.\n'
}

install_docker() {
  require_command docker
  docker compose version >/dev/null 2>&1 \
    || die "Docker Compose is required (use Docker Desktop or the Compose plugin)"
  ensure_env_file

  if [ -n "$IMAGE" ]; then
    export ANIU_IMAGE="$IMAGE"
    printf 'Pulling image: %s\n' "$IMAGE"
    docker compose --env-file "$ENV_FILE" pull
    docker compose --env-file "$ENV_FILE" up -d --no-build
  else
    printf '%s\n' 'Building the local Docker image'
    docker compose --env-file "$ENV_FILE" build
    docker compose --env-file "$ENV_FILE" up -d
  fi

  printf 'Aniu is running locally at http://127.0.0.1:%s\n' "$(port_from_env)"
}

install_frontend() {
  mkdir -p "$LOCAL_DIR/npm-cache" "$LOCAL_DIR/tsbuildinfo"
  npm_config_cache="$LOCAL_DIR/npm-cache" npm --prefix "$ROOT_DIR/frontend" ci --include=dev
  npm_config_cache="$LOCAL_DIR/npm-cache" npm --prefix "$ROOT_DIR/frontend" run build
}

install_systemd() {
  require_command systemctl
  ensure_env_file
  [ "$START_SOURCE" -eq 0 ] || die "Use either --start or --systemd, not both"

  local service_name="${ANIU_SYSTEMD_SERVICE:-aniu.service}"
  local service_user="${ANIU_SYSTEMD_USER:-${SUDO_USER:-$(id -un)}}"
  local port="${ANIU_SYSTEMD_PORT:-8000}"
  local lan_mode="${ANIU_SYSTEMD_LAN:-0}"
  local allowed_hosts="${ANIU_SYSTEMD_ALLOWED_HOSTS:-localhost,127.0.0.1,localhost.localdomain}"
  local cors_origins="${ANIU_SYSTEMD_CORS_ORIGINS:-http://localhost:$port,http://127.0.0.1:$port}"
  local log_level="${ANIU_SYSTEMD_LOG_LEVEL:-info}"
  local bind_host="127.0.0.1"
  local lan_flag="--no-lan"
  local unit_file

  case "$lan_mode" in
    1|true|yes|on) lan_mode=1 ;;
    0|false|no|off|"") lan_mode=0 ;;
    *) die "ANIU_SYSTEMD_LAN must be a boolean" ;;
  esac
  if [ "$lan_mode" -eq 1 ]; then
    local lan_host
    lan_host="$(detect_lan_ipv4)" || die "Could not determine a private LAN IPv4 address"
    allowed_hosts="${ANIU_SYSTEMD_ALLOWED_HOSTS:-localhost,127.0.0.1,$lan_host}"
    bind_host="${ANIU_SYSTEMD_HOST:-0.0.0.0}"
    lan_flag="--lan"
  fi

  case "$service_name" in
    ""|*[!A-Za-z0-9_.@-]*) die "ANIU_SYSTEMD_SERVICE contains invalid characters" ;;
  esac
  case "$service_user" in
    ""|*[!A-Za-z0-9_-]*) die "ANIU_SYSTEMD_USER contains invalid characters" ;;
  esac
  id "$service_user" >/dev/null 2>&1 || die "Systemd user does not exist: $service_user"
  case "$port" in
    ""|*[!0-9]*) die "ANIU_SYSTEMD_PORT must be a number" ;;
  esac
  if [ "$port" -lt 1 ] || [ "$port" -gt 65535 ]; then
    die "ANIU_SYSTEMD_PORT must be between 1 and 65535"
  fi
  case "$allowed_hosts" in
    ""|*[!A-Za-z0-9.,:_-]*) die "ANIU_SYSTEMD_ALLOWED_HOSTS contains invalid characters" ;;
  esac
  case "$cors_origins" in
    *[[:space:]]*) die "ANIU_SYSTEMD_CORS_ORIGINS cannot contain spaces" ;;
  esac
  case "$ROOT_DIR:$VENV_DIR:$ENV_FILE" in
    *[!A-Za-z0-9_./:-]*) die "systemd installation paths contain unsupported characters" ;;
  esac

  unit_file="$(mktemp)"
  cat > "$unit_file" <<EOF
[Unit]
Description=Aniu application
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$service_user
WorkingDirectory=$ROOT_DIR
EnvironmentFile=$ENV_FILE
Environment=PATH=$VENV_DIR/bin:/usr/local/bin:/usr/bin:/bin
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPYCACHEPREFIX=/var/lib/aniu/pycache
Environment=ANIU_DATA_DIR=/var/lib/aniu
Environment=ANIU_FRONTEND_DIST=$ROOT_DIR/frontend/dist
Environment=ANIU_SERVE_FRONTEND=1
Environment=ANIU_ENABLE_SCHEDULER=1
Environment=ANIU_LAN=$lan_mode
Environment=ANIU_ALLOWED_HOSTS=$allowed_hosts
Environment=ANIU_CORS_ORIGINS=$cors_origins
Environment=ANIU_LOG_LEVEL=$log_level
StateDirectory=aniu
ExecStart=$VENV_DIR/bin/python -m backend.serve --host $bind_host --port $port $lan_flag --allowed-host $allowed_hosts
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

  if ! run_privileged install -d -m 0755 "$SYSTEMD_DIR"; then
    rm -f "$unit_file"
    die "Cannot create systemd unit directory: $SYSTEMD_DIR"
  fi
  if ! run_privileged install -m 0644 "$unit_file" "$SYSTEMD_DIR/$service_name"; then
    rm -f "$unit_file"
    die "Cannot install systemd unit: $SYSTEMD_DIR/$service_name"
  fi
  rm -f "$unit_file"
  run_privileged systemctl daemon-reload
  run_privileged systemctl enable --now "$service_name"
  printf 'Installed and started systemd service: %s\n' "$service_name"
  printf 'Check status with: systemctl status %s\n' "$service_name"
}

install_source() {
  require_command python3
  require_command npm
  check_python_version
  ensure_local_dir

  if [ ! -x "$VENV_DIR/bin/python" ]; then
    printf 'Creating Python environment: %s\n' "$VENV_DIR"
    python3 -m venv "$VENV_DIR"
  fi

  mkdir -p "$LOCAL_DIR/pip-cache" "$LOCAL_DIR/pycache"
  export PIP_CACHE_DIR="$LOCAL_DIR/pip-cache"
  export PYTHONPYCACHEPREFIX="$LOCAL_DIR/pycache"
  "$VENV_DIR/bin/python" -m pip install \
    --require-hashes -r "$ROOT_DIR/requirements.lock"
  (
    cd "$ROOT_DIR"
    "$VENV_DIR/bin/python" -m pip install --no-deps -e .
  )
  install_frontend

  if [ "$INSTALL_SYSTEMD" -eq 1 ]; then
    install_systemd
    return
  fi

  if [ "$START_SOURCE" -eq 1 ]; then
    export ANIU_DATA_DIR="${ANIU_DATA_DIR:-$ROOT_DIR/.aniu}"
    export ANIU_SERVE_FRONTEND=1
    exec "$VENV_DIR/bin/python" -m backend.serve \
      --host "${ANIU_HOST:-127.0.0.1}" \
      --port "${ANIU_PORT:-8000}"
  fi

  cat <<EOF

Source installation completed.

Start the application with:
  ANIU_SERVE_FRONTEND=1 ANIU_DATA_DIR=$ROOT_DIR/.aniu \
    $VENV_DIR/bin/python -m backend.serve --host 127.0.0.1 --port 8000

Or run the local development launcher when it is available:
  $VENV_DIR/bin/python $ROOT_DIR/.aniu/dev.py
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    docker|--docker)
      MODE=docker
      ;;
    source|--source)
      MODE=source
      ;;
    clean|--clean)
      MODE=clean
      ;;
    --image)
      [ "$#" -ge 2 ] || die "--image requires a value"
      shift
      IMAGE="$1"
      ;;
    --start)
      START_SOURCE=1
      ;;
    --systemd)
      INSTALL_SYSTEMD=1
      ;;
    --no-start)
      START_SOURCE=0
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown option: $1 (use --help for usage)"
      ;;
  esac
  shift
done

if [ "$INSTALL_SYSTEMD" -eq 1 ] && [ "$MODE" != source ]; then
  die "--systemd can only be used with source mode"
fi
if [ "$INSTALL_SYSTEMD" -eq 1 ] && [ "$START_SOURCE" -eq 1 ]; then
  die "Use either --start or --systemd, not both"
fi

cd "$ROOT_DIR"
case "$MODE" in
  docker)
    install_docker
    ;;
  source)
    install_source
    ;;
  clean)
    clean_local_artifacts
    ;;
  *)
    die "Unsupported installation mode: $MODE"
    ;;
esac
