"""Container image security and data-directory startup contracts."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_container_entrypoint_repairs_data_mount_then_drops_privileges() -> None:
    entrypoint = (PROJECT_ROOT / "docker-entrypoint.sh").read_text()

    assert "DATA_DIR=/app/data" in entrypoint
    assert 'chown "$RUNTIME_UID:$RUNTIME_GID" "$DATA_DIR"' in entrypoint
    assert 'chmod 0700 "$DATA_DIR"' in entrypoint
    assert "umask 077" in entrypoint
    assert '--reuid="$RUNTIME_UID"' in entrypoint
    assert '--regid="$RUNTIME_GID"' in entrypoint


def test_dockerfile_uses_privilege_dropping_entrypoint() -> None:
    dockerfile = (PROJECT_ROOT / "Dockerfile").read_text()

    assert "COPY docker-entrypoint.sh /usr/local/bin/aniu-entrypoint" in dockerfile
    assert "chmod 0755 /usr/local/bin/aniu-entrypoint" in dockerfile
    assert "USER root" in dockerfile
    assert (
        'ENTRYPOINT ["/usr/local/bin/aniu-entrypoint", "python", "-m", "backend.serve"]'
    ) in dockerfile
