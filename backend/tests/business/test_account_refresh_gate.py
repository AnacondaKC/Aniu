"""Tests for process-local account refresh protection."""

from backend.business.account.runtime import AccountRefreshGate


def test_account_refresh_gate_allows_one_refresh_per_ten_seconds() -> None:
    clock = [100.0]
    gate = AccountRefreshGate(now=lambda: clock[0])

    assert gate.allow()
    assert not gate.allow()

    clock[0] += 9.9
    assert not gate.allow()

    clock[0] += 0.1
    assert gate.allow()
