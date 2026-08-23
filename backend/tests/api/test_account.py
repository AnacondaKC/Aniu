"""API tests for account overview / positions / orders endpoints."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date

import pytest
from httpx import ASGITransport, AsyncClient

from backend.api.deps import get_account_app_service, get_session_factory
from backend.business.account import (
    AccountCacheState,
    AccountSnapshot,
    PortfolioOrderSnapshot,
    PositionSnapshot,
)
from backend.business.account.runtime import AccountRefreshGate
from backend.business.account.service import AccountAppService
from backend.business.settings import AppSettings
from backend.business.shared import ServiceConfigurationError
from backend.infra.repositories import SettingsRepository
from backend.main import app
from backend.stock_api.mx import MxMoniClient


class FakePortfolioClient:
    async def get_account_snapshot(self) -> AccountSnapshot:
        return AccountSnapshot(
            total_asset=100000.0,
            available_cash=25000.0,
            frozen_cash=1000.0,
            market_value=74000.0,
            total_profit=8000.0,
            daily_profit=500.0,
            operation_days=12,
            open_date=date(2026, 4, 12),
            initial_capital=200000.0,
            net_value=1.0,
            position_ratio=0.37,
        )

    async def get_positions(self) -> list[PositionSnapshot]:
        return [
            PositionSnapshot(
                symbol="600519",
                stock_name="贵州茅台",
                quantity=100,
                avg_cost=1600.0,
                current_price=1700.0,
                market_value=170000.0,
                profit_ratio=0.0625,
            )
        ]

    async def get_orders(self) -> list[PortfolioOrderSnapshot]:
        return [
            PortfolioOrderSnapshot(
                order_id="order-1",
                symbol="000001",
                stock_name="平安银行",
                direction="BUY",
                quantity=200,
                order_price=3.874,
                status="PENDING",
                filled_quantity=0,
                filled_price=None,
            )
        ]


class BrokenPortfolioClient(FakePortfolioClient):
    async def get_account_snapshot(self) -> AccountSnapshot:
        raise ServiceConfigurationError("MX_APIKEY is not configured")


class FakeAccountCacheRepo:
    def __init__(self) -> None:
        self.state: AccountCacheState | None = None
        self.positions: list[PositionSnapshot] = []
        self.orders: list[PortfolioOrderSnapshot] = []

    async def get_state(self) -> AccountCacheState | None:
        return self.state

    async def get_account_snapshot(self) -> AccountSnapshot | None:
        return None if self.state is None else self.state.to_snapshot()

    async def list_positions(self) -> list[PositionSnapshot]:
        return list(self.positions)

    async def list_orders(self, *, limit: int) -> list[PortfolioOrderSnapshot]:
        return list(self.orders[:limit])

    async def count_filled_orders_on_date(self, day) -> int:
        return sum(
            1
            for order in self.orders
            if (order.updated_at or order.submitted_at) is not None
            and (order.updated_at or order.submitted_at).date() == day
            and order.filled_quantity > 0
        )

    async def replace_cache(
        self,
        snapshot: AccountSnapshot,
        positions: list[PositionSnapshot],
        orders: list[PortfolioOrderSnapshot],
        *,
        attempted_at,
    ) -> AccountCacheState:
        self.state = AccountCacheState(
            total_asset=snapshot.total_asset,
            available_cash=snapshot.available_cash,
            frozen_cash=snapshot.frozen_cash,
            market_value=snapshot.market_value,
            total_profit=snapshot.total_profit,
            daily_profit=snapshot.daily_profit,
            operation_days=snapshot.operation_days,
            open_date=snapshot.open_date,
            initial_capital=snapshot.initial_capital,
            net_value=snapshot.net_value,
            position_ratio=snapshot.position_ratio,
            captured_at=snapshot.captured_at,
            last_refresh_attempt_at=attempted_at,
            last_refresh_succeeded_at=attempted_at,
            last_refresh_error=None,
        )
        self.positions = list(positions)
        self.orders = list(orders)
        return self.state

    async def record_refresh_failure(
        self,
        *,
        attempted_at,
        error_message: str,
    ) -> AccountCacheState:
        previous = self.state or AccountCacheState()
        self.state = AccountCacheState(
            total_asset=previous.total_asset,
            available_cash=previous.available_cash,
            frozen_cash=previous.frozen_cash,
            market_value=previous.market_value,
            total_profit=previous.total_profit,
            daily_profit=previous.daily_profit,
            operation_days=previous.operation_days,
            open_date=previous.open_date,
            initial_capital=previous.initial_capital,
            net_value=previous.net_value,
            position_ratio=previous.position_ratio,
            captured_at=previous.captured_at,
            last_refresh_attempt_at=attempted_at,
            last_refresh_succeeded_at=previous.last_refresh_succeeded_at,
            last_refresh_error=error_message,
        )
        return self.state


class FakeCommitter:
    async def commit(self) -> None:
        return None


@pytest.fixture
async def api_client(session_factory) -> AsyncIterator[AsyncClient]:
    # Auth dependencies need a real session factory; open-mode (no users) keeps
    # routes callable without login for service-override tests.
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    app.state.runtime.session_factory = session_factory
    app.state.runtime.model_connectivity_tester = object()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()
    if hasattr(app.state, "session_factory"):
        delattr(app.state, "session_factory")


@pytest.fixture
async def db_backed_api_client(session_factory) -> AsyncIterator[AsyncClient]:
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    app.state.runtime.session_factory = session_factory
    app.state.runtime.account_refresh_gate = AccountRefreshGate()
    mx_http_client = AsyncClient()
    app.state.runtime.mx_http_client = mx_http_client
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()
    await mx_http_client.aclose()
    app.state.runtime.mx_http_client = None
    if hasattr(app.state, "session_factory"):
        delattr(app.state, "session_factory")


@pytest.mark.asyncio
async def test_account_dashboard_returns_overview_positions_and_orders(
    api_client: AsyncClient,
) -> None:
    app.dependency_overrides[get_account_app_service] = lambda: AccountAppService(
        portfolio_client=FakePortfolioClient(),
        account_cache_repo=FakeAccountCacheRepo(),
        committer=FakeCommitter(),
    )

    dashboard = await api_client.get("/api/aniu/account/dashboard")

    assert dashboard.status_code == 200
    assert dashboard.json()["overview"]["total_asset"] == 100000.0
    assert dashboard.json()["overview"]["operation_days"] == 12
    assert dashboard.json()["positions"][0]["symbol"] == "600519"
    assert dashboard.json()["orders"][0]["order_id"] == "order-1"
    assert dashboard.json()["orders"][0]["order_price"] == 3.874


@pytest.mark.asyncio
async def test_account_dashboard_returns_503_for_configuration_error(
    api_client: AsyncClient,
) -> None:
    app.dependency_overrides[get_account_app_service] = lambda: AccountAppService(
        portfolio_client=BrokenPortfolioClient(),
        account_cache_repo=FakeAccountCacheRepo(),
        committer=FakeCommitter(),
    )

    response = await api_client.get("/api/aniu/account/dashboard")

    assert response.status_code == 503
    body = response.json()
    message = body.get("detail") or body.get("error", {}).get("message", "")
    assert "MX_APIKEY" in message


@pytest.mark.asyncio
async def test_account_refresh_endpoint_uses_saved_settings_mx_api_key(
    db_backed_api_client: AsyncClient,
    session_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with session_factory() as session:
        await SettingsRepository(session).save(AppSettings(mx_api_key="mx-test-key"))
        await session.commit()

    async def fake_post(self: MxMoniClient, endpoint: str, body: dict[str, object]):
        del body
        assert self.api_key_resolver is not None
        assert await self.api_key_resolver() == "mx-test-key"
        if endpoint.endswith("/balance"):
            return {
                "totalAssets": 88000.0,
                "availBalance": 12000.0,
                "frozenBalance": 500.0,
                "marketValue": 75500.0,
                "totalProfit": 3200.0,
                "dailyProfit": 180.0,
                "currencyUnit": 1,
            }
        if endpoint.endswith("/positions") or endpoint.endswith("/orders"):
            return []
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    monkeypatch.setattr(MxMoniClient, "_post", fake_post)

    response = await db_backed_api_client.post("/api/aniu/account/refresh")
    dashboard = await db_backed_api_client.get("/api/aniu/account/dashboard")

    assert response.status_code == 200
    assert response.json()["status"] == "refreshed"
    assert dashboard.status_code == 200
    assert dashboard.json()["overview"]["available_cash"] == 12000.0


@pytest.mark.asyncio
async def test_account_refresh_endpoint_throttles_repeated_requests(
    db_backed_api_client: AsyncClient,
    session_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with session_factory() as session:
        await SettingsRepository(session).save(AppSettings(mx_api_key="mx-test-key"))
        await session.commit()

    calls: list[str] = []

    async def fake_post(self: MxMoniClient, endpoint: str, body: dict[str, object]):
        del body
        calls.append(endpoint)
        if endpoint.endswith("/balance"):
            return {
                "totalAssets": 1000.0,
                "availBalance": 800.0,
                "frozenBalance": 0.0,
                "marketValue": 200.0,
                "totalProfit": 0.0,
                "dailyProfit": 0.0,
                "currencyUnit": 1,
            }
        if endpoint.endswith("/positions"):
            return []
        if endpoint.endswith("/orders"):
            return []
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    monkeypatch.setattr(MxMoniClient, "_post", fake_post)

    first = await db_backed_api_client.post("/api/aniu/account/refresh")
    second = await db_backed_api_client.post("/api/aniu/account/refresh")

    assert first.status_code == 200
    assert first.json()["status"] == "refreshed"
    assert second.status_code == 429
    assert second.json()["error"]["message"] == "10 秒内只能刷新一次，请稍后重试。"
    assert calls == [
        "/api/claw/mockTrading/balance",
        "/api/claw/mockTrading/positions",
        "/api/claw/mockTrading/orders",
    ]
