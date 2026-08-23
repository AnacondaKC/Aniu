#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
LOCAL_DIR="$ROOT_DIR/.aniu/local"
VENV_DIR="$LOCAL_DIR/.venv"

fail() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

[ "$#" -eq 0 ] || fail "This installer does not accept arguments. Run: ./install.sh"

require_command python3
require_command npm
python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)' \
  || fail "Python 3.12 or newer is required"

mkdir -p "$LOCAL_DIR"
chmod 700 "$ROOT_DIR/.aniu" "$LOCAL_DIR"

if [ ! -x "$VENV_DIR/bin/python" ]; then
  printf 'Creating Python environment: %s\n' "$VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

mkdir -p "$LOCAL_DIR/pip-cache" "$LOCAL_DIR/npm-cache" "$LOCAL_DIR/pycache"
export PIP_CACHE_DIR="$LOCAL_DIR/pip-cache"
export PYTHONPYCACHEPREFIX="$LOCAL_DIR/pycache"

printf '%s\n' 'Installing Python dependencies'
"$VENV_DIR/bin/python" -m pip install --require-hashes -r "$ROOT_DIR/requirements.lock"
"$VENV_DIR/bin/python" -m pip install --no-deps -e "$ROOT_DIR"

printf '%s\n' 'Installing and building frontend'
npm_config_cache="$LOCAL_DIR/npm-cache" npm --prefix "$ROOT_DIR/frontend" ci --include=dev
npm_config_cache="$LOCAL_DIR/npm-cache" npm --prefix "$ROOT_DIR/frontend" run build

cd "$ROOT_DIR"
export ANIU_DATA_DIR="${ANIU_DATA_DIR:-$ROOT_DIR/.aniu}"
export ANIU_SERVE_FRONTEND=1

printf 'Aniu is running at http://%s:%s\n' "${ANIU_HOST:-127.0.0.1}" "${ANIU_PORT:-8000}"
exec "$VENV_DIR/bin/python" -m backend.serve \
  --host "${ANIU_HOST:-127.0.0.1}" \
  --port "${ANIU_PORT:-8000}"
