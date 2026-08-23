#!/bin/sh
set -eu

DATA_DIR=/app/data
RUNTIME_UID=10001
RUNTIME_GID=10001

if [ "$(id -u)" -eq 0 ]; then
  if ! mkdir -p "$DATA_DIR" \
    || ! chown "$RUNTIME_UID:$RUNTIME_GID" "$DATA_DIR" \
    || ! chmod 0700 "$DATA_DIR"; then
    printf '%s\n' \
      "Aniu cannot initialize $DATA_DIR for its non-root runtime user." \
      "Ensure the mounted filesystem supports chown, or remove a custom user: override." >&2
    exit 1
  fi

  umask 077

  exec setpriv \
    --reuid="$RUNTIME_UID" \
    --regid="$RUNTIME_GID" \
    --init-groups \
    -- "$@"
fi

exec "$@"
