#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
PYTHON_BIN="${1:-python}"

if [ "$#" -gt 1 ]; then
  printf 'Usage: %s [python-executable]\n' "$0" >&2
  exit 2
fi

cd "$ROOT_DIR"
"$PYTHON_BIN" -m pip install --upgrade pip
if [ -f requirements.lock ]; then
  "$PYTHON_BIN" -m pip install --require-hashes -r requirements.lock
fi
"$PYTHON_BIN" -m pip install -e ".[dev]"
