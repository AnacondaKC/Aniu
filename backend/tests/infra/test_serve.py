"""Backend serve entrypoint network default tests."""

from __future__ import annotations

import os
import sys

import pytest

from backend import serve


def test_parser_defaults_to_loopback_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANIU_LAN", raising=False)
    monkeypatch.delenv("ANIU_HOST", raising=False)

    args = serve.build_parser().parse_args([])

    assert args.lan is False
    assert args.host is None


def test_main_uses_loopback_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(app: str, **options: object) -> None:
        captured["app"] = app
        captured.update(options)

    monkeypatch.delenv("ANIU_LAN", raising=False)
    monkeypatch.delenv("ANIU_HOST", raising=False)
    monkeypatch.delenv("ANIU_ALLOWED_HOSTS", raising=False)
    monkeypatch.setattr(serve.uvicorn, "run", fake_run)
    monkeypatch.setattr(sys, "argv", ["backend.serve"])

    serve.main()

    assert captured["app"] == "backend.main:app"
    assert captured["host"] == "127.0.0.1"
    assert os.environ["ANIU_LAN"] == "0"


def test_main_uses_discovered_lan_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(_app: str, **options: object) -> None:
        captured.update(options)

    monkeypatch.delenv("ANIU_LAN", raising=False)
    monkeypatch.delenv("ANIU_HOST", raising=False)
    monkeypatch.delenv("ANIU_ALLOWED_HOSTS", raising=False)
    monkeypatch.setattr(
        serve,
        "default_lan_allowed_hosts",
        lambda: ("192.168.1.20", "localhost", "127.0.0.1", "test", "::1"),
    )
    monkeypatch.setattr(serve.uvicorn, "run", fake_run)
    monkeypatch.setattr(sys, "argv", ["backend.serve", "--lan"])

    serve.main()

    assert captured["host"] == "0.0.0.0"
    assert os.environ["ANIU_LAN"] == "1"
    assert os.environ["ANIU_ALLOWED_HOSTS"] == (
        "192.168.1.20,localhost,127.0.0.1,test,::1"
    )


def test_lan_without_discoverable_host_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANIU_ALLOWED_HOSTS", raising=False)
    monkeypatch.setattr(
        serve, "default_lan_allowed_hosts", lambda: ("localhost", "127.0.0.1")
    )
    monkeypatch.setattr(sys, "argv", ["backend.serve", "--lan"])

    with pytest.raises(SystemExit):
        serve.main()


def test_explicit_external_host_keeps_loopback_auth_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run(_app: str, **options: object) -> None:
        captured.update(options)

    monkeypatch.delenv("ANIU_LAN", raising=False)
    monkeypatch.setattr(serve.uvicorn, "run", fake_run)
    monkeypatch.setattr(sys, "argv", ["backend.serve", "--host", "0.0.0.0"])

    serve.main()

    assert captured["host"] == "0.0.0.0"
    assert os.environ["ANIU_LAN"] == "0"
