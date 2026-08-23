import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter, useLocation } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DashboardPage, getMarketRefreshInterval } from "./dashboard-page";

const api = vi.hoisted(() => ({
  getAccountDashboard: vi.fn(),
  getMarketIndices: vi.fn(),
  getMarketDetails: vi.fn(),
  getSettings: vi.fn(),
  refreshAccountCache: vi.fn(),
}));

vi.mock("@/lib/api", () => api);

const dashboard = {
  overview: {
    total_asset: 120000,
    available_cash: 20000,
    frozen_cash: 0,
    market_value: 100000,
    total_profit: 20000,
    daily_profit: 1000,
    operation_days: 30,
    open_date: "2026-07-01",
    initial_capital: 100000,
    net_value: 1.2,
    position_ratio: 0.8333,
    performance_date: "2026-07-31",
    captured_at: "2026-07-31T08:00:00Z",
    total_return_rate: 0.2,
    current_net_value: 1.2,
    daily_return_rate: 0.01,
    today_trade_count: 1,
  },
  positions: [
    {
      symbol: "600000",
      stock_name: "浦发银行",
      quantity: 1000,
      available_quantity: 800,
      avg_cost: 9,
      current_price: 10,
      market_value: 10000,
      profit_ratio: 0.1111,
      day_profit: 150.5,
      captured_at: "2026-07-31T08:00:00Z",
    },
  ],
  orders: [
    {
      order_id: "order-1",
      symbol: "600000",
      stock_name: "浦发银行",
      direction: "BUY",
      quantity: 100,
      order_price: 10,
      filled_quantity: 100,
      filled_price: 10,
      status: "FILLED",
      submitted_at: "2026-07-31T07:30:00Z",
      updated_at: "2026-07-31T07:31:00Z",
    },
  ],
};

const market = {
  generated_at: "2026-07-31T08:00:00Z",
  indices: [
    {
      id: "sse",
      name: "上证指数",
      symbol: "000001.SH",
      price: 3280.5,
      previous_close: 3260,
      change: 20.5,
      change_percent: 0.63,
      high: 3290,
      low: 3250,
    },
    {
      id: "szse",
      name: "深证成指",
      symbol: "399001.SZ",
      price: 10000,
      previous_close: 9900,
      change: 100,
      change_percent: 1.01,
      high: 10100,
      low: 9800,
    },
  ],
  trends: [
    {
      id: "sse",
      points: [
        { time: "09:30", price: 3260, cumulative_amount: 100_000_000 },
        { time: "09:31", price: 3280.5, cumulative_amount: 110_000_000 },
        { time: "09:35", price: 3281, cumulative_amount: 150_000_000 },
      ],
    },
    {
      id: "szse",
      points: [
        { time: "09:30", price: 9900, cumulative_amount: 200_000_000 },
        { time: "09:31", price: 10000, cumulative_amount: 225_000_000 },
        { time: "09:35", price: 10001, cumulative_amount: 300_000_000 },
      ],
    },
  ],
  turnover: { today_amount: 123000000000 },
  breadth: { rising: 1576, falling: 3549, flat: 158 },
  rankings: {
    gainers: [{ name: "浦发银行", symbol: "600000", price: 10, change_percent: 5 }],
    losers: [],
    net_inflow: [],
    net_outflow: [],
  },
  hotspots: { industry: [], concept: [] },
  headlines: [],
  flash_news: [],
  errors: [],
};

function LocationDisplay() {
  const location = useLocation();
  return <output data-testid="location">{`${location.pathname}${location.search}`}</output>;
}

function renderPage({ mxApiKeyConfigured = true } = {}) {
  api.getSettings.mockResolvedValue({
    mx: { api_key_configured: mxApiKeyConfigured },
  });
  api.getMarketIndices.mockResolvedValue({
    generated_at: market.generated_at,
    indices: market.indices,
    trends: market.trends,
    errors: [{ resource: "indices", item_id: "chinext", message: "创业板指数暂不可用" }],
  });
  api.getMarketDetails.mockResolvedValue({
    generated_at: market.generated_at,
    turnover: market.turnover,
    breadth: market.breadth,
    rankings: market.rankings,
    hotspots: market.hotspots,
    headlines: market.headlines,
    flash_news: market.flash_news,
    errors: [],
  });
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <MemoryRouter initialEntries={["/"]}>
      <QueryClientProvider client={queryClient}>
        <DashboardPage />
        <LocationDisplay />
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => vi.clearAllMocks());

describe("DashboardPage", () => {
  it("prompts for the MX key without requesting account data", async () => {
    const user = userEvent.setup();

    renderPage({ mxApiKeyConfigured: false });

    expect(await screen.findByText("尚未配置妙想 API 密钥")).toBeInTheDocument();
    expect(
      screen.getByText("配置后即可读取模拟组合的账户资产、持仓和委托数据。"),
    ).toBeInTheDocument();
    expect(api.getAccountDashboard).not.toHaveBeenCalled();

    await user.click(screen.getByRole("link", { name: "配置密钥" }));
    expect(screen.getByTestId("location")).toHaveTextContent("/settings");
  });

  it("loads cached data and refreshes only on explicit request", async () => {
    const user = userEvent.setup();
    api.getAccountDashboard.mockResolvedValue(dashboard);
    api.refreshAccountCache.mockResolvedValue({
      status: "fresh",
      message: "账户数据已刷新",
      captured_at: "2026-07-31T08:00:00Z",
      last_refresh_attempt_at: "2026-07-31T08:00:00Z",
      last_refresh_succeeded_at: "2026-07-31T08:00:00Z",
    });

    renderPage();

    expect(await screen.findByText("投资总览")).toBeInTheDocument();
    expect((await screen.findAllByText("浦发银行")).length).toBeGreaterThan(0);
    expect(screen.getByText("当日盈亏")).toBeInTheDocument();
    expect(screen.getByText("¥150.50")).toBeInTheDocument();
    expect(screen.getByText("买入")).toBeInTheDocument();

    const refreshTimePattern = /^最近刷新：\d{2}-\d{2} \d{2}:\d{2}$/;
    const refreshTimeClasses = [
      "text-muted-foreground",
      "min-w-0",
      "flex-1",
      "truncate",
      "text-right",
      "text-xs",
      "tabular-nums",
    ];
    expect(screen.getByText(refreshTimePattern)).toHaveClass(...refreshTimeClasses);

    await user.click(screen.getByRole("tab", { name: "行情总览" }));
    expect((await screen.findAllByText("上证指数")).length).toBeGreaterThan(0);
    expect(screen.getByText(refreshTimePattern)).toHaveClass(...refreshTimeClasses);
    expect(screen.getByText("个股涨幅")).toBeInTheDocument();
    expect(screen.getByText("两市成交总额")).toBeInTheDocument();
    expect(screen.getByLabelText("沪深A股涨跌：上涨 1576，下跌 3549")).toBeInTheDocument();
    const sparkline = screen.getByRole("img", { name: "上证指数当日分时走势" });
    expect(sparkline.querySelector("polyline")?.getAttribute("points")).toMatch(/^0\.00,/);
    const indexTurnoverChart = screen.getByRole("img", {
      name: "上证指数分时成交额变化，共 2 个时段",
    });
    const indexBars = indexTurnoverChart.querySelectorAll('[data-slot="index-turnover-bar"]');
    expect(indexBars).toHaveLength(2);
    expect(indexBars[0]).toHaveStyle({ left: "0%" });
    expect(screen.getAllByText("09:30").length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText("15:00").length).toBeGreaterThanOrEqual(2);

    await user.click(screen.getByRole("tab", { name: "账户总览" }));
    await waitFor(() => expect(api.refreshAccountCache).toHaveBeenCalledTimes(1));

    await user.click(screen.getByRole("button", { name: "刷新数据" }));
    await waitFor(() => expect(api.refreshAccountCache).toHaveBeenCalledTimes(2));
  });

  it("uses Shanghai trading sessions for market polling and resumes at boundaries", () => {
    expect(getMarketRefreshInterval(new Date("2026-07-31T02:00:00Z"))).toBe(5_000);
    expect(getMarketRefreshInterval(new Date("2026-07-31T04:00:00Z"))).toBe(60 * 60 * 1000);
    expect(getMarketRefreshInterval(new Date("2026-08-01T03:00:00Z"))).toBe(46.5 * 60 * 60 * 1000);
  });

  it("shows the same refresh interaction as call logs", async () => {
    const user = userEvent.setup();
    let resolveRefresh:
      | ((value: {
          status: string;
          message: string;
          captured_at: string;
          last_refresh_attempt_at: string;
          last_refresh_succeeded_at: string;
        }) => void)
      | undefined;
    const pendingRefresh = new Promise<{
      status: string;
      message: string;
      captured_at: string;
      last_refresh_attempt_at: string;
      last_refresh_succeeded_at: string;
    }>((resolve) => {
      resolveRefresh = resolve;
    });
    api.getAccountDashboard.mockResolvedValue(dashboard);
    api.refreshAccountCache.mockResolvedValue({
      status: "fresh",
      message: "账户数据已刷新",
      captured_at: "2026-07-31T08:00:00Z",
      last_refresh_attempt_at: "2026-07-31T08:00:00Z",
      last_refresh_succeeded_at: "2026-07-31T08:00:00Z",
    });

    renderPage();

    await screen.findByText("投资总览");
    await waitFor(() => expect(api.refreshAccountCache).toHaveBeenCalledTimes(1));
    api.refreshAccountCache.mockReturnValueOnce(pendingRefresh);
    const refreshButton = screen.getByRole("button", { name: "刷新数据" });
    await user.click(refreshButton);

    await waitFor(() => expect(refreshButton).toHaveAttribute("aria-busy", "true"));
    expect(refreshButton).toBeDisabled();
    expect(refreshButton).toHaveClass("cursor-wait");
    expect(refreshButton.querySelector("svg")).toHaveClass("animate-spin");

    resolveRefresh?.({
      status: "fresh",
      message: "账户数据已刷新",
      captured_at: "2026-07-31T08:00:00Z",
      last_refresh_attempt_at: "2026-07-31T08:00:00Z",
      last_refresh_succeeded_at: "2026-07-31T08:00:00Z",
    });

    await waitFor(() => expect(refreshButton).toHaveAttribute("aria-busy", "false"), {
      timeout: 2_000,
    });
    expect(refreshButton).toBeEnabled();
  });
});
