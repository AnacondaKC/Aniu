import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { StockApiSettingsPage } from "./stock-api-settings-page";

class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}

vi.stubGlobal("ResizeObserver", ResizeObserverMock);

const api = vi.hoisted(() => ({
  getStockApiSettings: vi.fn(),
  listStockApiLogs: vi.fn(),
}));

vi.mock("@/lib/api", () => api);

const stockApiSettings = {
  public_stock: {
    name: "公开数据",
    summary: "个股与指数行情无需密钥并自动选择腾讯、新浪和东方财富。",
    providers: ["tencent", "sina", "eastmoney"],
    features: ["实时行情", "K 线与分时", "资金流向"],
    tools: [
      {
        tool_name: "stock_quote",
        name: "实时行情",
        summary: "查询一只或多只沪深 A 股的标准化实时行情快照。",
        actions: ["行情快照"],
        providers: ["tencent", "sina", "eastmoney"],
      },
      {
        tool_name: "query_kline",
        name: "K 线走势",
        summary: "统一查询个股与指数的日、周、月及分钟 K 线。",
        actions: ["个股 K 线", "指数 K 线"],
        providers: ["tencent", "sina", "eastmoney"],
      },
      {
        tool_name: "market_snapshot",
        name: "行情查询",
        summary: "聚合七大指数实时行情、近 5 根日 K 线与近期市场策略研报。",
        actions: ["七大指数", "日 K 线", "策略研报"],
        providers: ["tencent", "sina", "eastmoney"],
      },
      {
        tool_name: "portfolio_stock_snapshot",
        name: "持仓查询",
        summary: "按持仓市值分页聚合模拟组合的行情、K 线、资金流、财务和资讯。",
        actions: ["持仓分页", "个股研判", "ETF 行情资讯"],
        providers: ["tencent", "sina", "eastmoney", "mx"],
      },
      {
        tool_name: "stock_analysis",
        name: "个股查询",
        summary: "聚合单只 A 股的行情、资金、基本数据、研报预测和资讯。",
        actions: ["行情 K 线", "资金财务", "研报资讯"],
        providers: ["tencent", "sina", "eastmoney"],
      },
      {
        tool_name: "industry_snapshot",
        name: "热度板块",
        summary: "聚合行业和概念板块当日资金流前 10 名与市场要闻前 20 条。",
        actions: ["行业资金流", "概念资金流", "市场要闻"],
        providers: ["eastmoney"],
      },
    ],
  },
  mx: {
    interfaces: [
      {
        id: "news",
        name: "资讯搜索",
        summary: "检索财经新闻、公告、研报和政策信息。",
        features: ["财经新闻"],
        examples: ["半导体行业最新研报"],
        access_modes: ["read"],
      },
      {
        id: "data",
        name: "金融数据查询",
        summary: "查询股票、指数、行情和财务数据。",
        features: ["股票行情"],
        examples: ["贵州茅台最新股价"],
        access_modes: ["read"],
      },
      {
        id: "portfolio",
        name: "模拟交易",
        summary: "查询模拟组合并提交交易或撤单。",
        features: ["委托、订单与成交", "买入与卖出"],
        examples: ["查询账户资金", "买入 600519 1700 100"],
        access_modes: ["read", "write"],
      },
    ],
  },
} as const;

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <StockApiSettingsPage />
    </QueryClientProvider>,
  );
}

afterEach(() => vi.clearAllMocks());

describe("StockApiSettingsPage", () => {
  it("shows data tools, system tools, and logs in the tool management navigation", async () => {
    const user = userEvent.setup();
    api.getStockApiSettings.mockResolvedValue(stockApiSettings);
    api.listStockApiLogs.mockResolvedValue({
      items: [],
      total: 0,
      summary: { total_calls: 0, success_calls: 0, failed_calls: 0, average_duration_ms: 0 },
    });

    renderPage();

    const tabList = await screen.findByRole("tablist", { name: "工具管理导航" });
    expect(tabList.parentElement?.tagName).toBe("ASIDE");
    const tabs = within(tabList).getAllByRole("tab");
    expect(tabs.map((tab) => tab.textContent)).toEqual(["数据工具", "系统工具", "调用日志"]);
    expect(screen.getByRole("tabpanel", { name: "数据工具" })).toBeInTheDocument();
    const directory = screen.getByRole("region", { name: "数据工具" });
    expect(screen.queryByRole("heading", { name: "公开数据" })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "妙想接口" })).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "公开数据目录" })).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "妙想工具目录" })).not.toBeInTheDocument();

    const cards = within(directory).getAllByRole("article");
    expect(cards).toHaveLength(9);
    const quoteCard = cards[0];
    if (!quoteCard) throw new Error("缺少实时行情卡片");
    expect(quoteCard).toHaveClass("min-w-0", "px-4", "py-3");
    const quoteSummary = within(quoteCard).getByText(
      "查询一只或多只沪深 A 股的标准化实时行情快照。",
    );
    expect(quoteSummary).toHaveClass("truncate");
    expect(quoteSummary).toHaveAttribute("title", "查询一只或多只沪深 A 股的标准化实时行情快照。");
    expect(within(quoteCard).getByLabelText("实时行情数据来源")).toHaveTextContent(
      "腾讯财经新浪财经东方财富",
    );
    const klineCard = cards[1];
    if (!klineCard) throw new Error("缺少 K 线走势卡片");
    expect(within(klineCard).getByLabelText("K 线走势数据来源")).toHaveTextContent(
      "腾讯财经新浪财经东方财富",
    );
    expect(screen.queryByText(/^自动路由$/)).not.toBeInTheDocument();
    expect(screen.queryByText("资金流向")).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "实时行情" })).toBeInTheDocument();
    expect(screen.queryByText("妙想 MX")).not.toBeInTheDocument();

    const marketSnapshotCard = cards[2];
    if (!marketSnapshotCard) throw new Error("缺少行情查询卡片");
    expect(within(marketSnapshotCard).getByLabelText("行情查询数据来源")).toHaveTextContent(
      "腾讯财经新浪财经东方财富",
    );

    const firstMxCard = cards[6];
    if (!firstMxCard) throw new Error("缺少资讯搜索卡片");
    expect(firstMxCard).toHaveClass("min-w-0", "px-4", "py-3");
    const firstMxSummary = within(firstMxCard).getByText("检索财经新闻、公告、研报和政策信息。");
    expect(firstMxSummary).toHaveClass("truncate");
    expect(firstMxSummary).toHaveAttribute("title", "检索财经新闻、公告、研报和政策信息。");
    expect(within(firstMxCard).getByLabelText("资讯搜索数据来源")).toHaveTextContent("妙想接口");
    expect(within(directory).queryByText(/^查询$/)).not.toBeInTheDocument();
    expect(within(directory).queryByText(/^交易$/)).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "资讯搜索" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "模拟交易" })).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "系统工具" }));
    const systemTools = await screen.findByRole("region", { name: "系统工具" });
    expect(within(systemTools).getAllByRole("article")).toHaveLength(2);
    expect(within(systemTools).getByRole("heading", { name: "记忆查询" })).toBeInTheDocument();
    expect(within(systemTools).getByText("memory_read")).toBeInTheDocument();
    expect(within(systemTools).getByRole("heading", { name: "记忆写入" })).toBeInTheDocument();
    expect(within(systemTools).getByText("memory_write")).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "调用日志" }));
    expect(await screen.findByRole("region", { name: "数据工具调用日志" })).toBeInTheDocument();
  });

  it("shows and filters logs by the sixteen data-tool groups", async () => {
    const user = userEvent.setup();
    api.getStockApiSettings.mockResolvedValue(stockApiSettings);
    api.listStockApiLogs.mockResolvedValue({
      items: [
        {
          id: 1,
          tool_source: "public",
          tool_id: "query_kline",
          tool_name: "K 线走势",
          parameters: {
            instrument: "000001.SH",
            period: "day",
            limit: 30,
            instrument_type: "index",
            resolved_instrument: "000001.SH",
            data_source: "public",
          },
          status: "success",
          duration_ms: 24,
          response_characters: 12_345,
          error_message: null,
          created_at: "2026-08-03T08:00:00Z",
        },
        {
          id: 2,
          tool_source: "aggregate",
          tool_id: "market_snapshot",
          tool_name: "行情查询",
          parameters: {},
          status: "success",
          duration_ms: 48,
          response_characters: 13_259,
          error_message: null,
          created_at: "2026-08-03T08:01:00Z",
        },
      ],
      total: 2,
      summary: { total_calls: 2, success_calls: 2, failed_calls: 0, average_duration_ms: 36 },
    });

    renderPage();
    await user.click(await screen.findByRole("tab", { name: "调用日志" }));

    expect(await screen.findByText("公开数据")).toBeInTheDocument();
    expect(screen.getByText("聚合研判")).toBeInTheDocument();
    expect(screen.getByText("K 线走势")).toBeInTheDocument();
    expect(
      screen.getByTitle(
        "查询标的：000001.SH；周期：day；数量上限：30；标的类型：指数；规范标的：000001.SH；数据来源：公开数据",
      ),
    ).toBeInTheDocument();
    expect(screen.queryByText("quote.snapshot")).not.toBeInTheDocument();
    expect(screen.queryByText("东方财富")).not.toBeInTheDocument();
    expect(screen.queryByText("返回")).not.toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "返回字符" })).toBeInTheDocument();
    expect(screen.getByText("12,345")).toBeInTheDocument();
    expect(screen.getByText("13,259")).toBeInTheDocument();
    expect(api.listStockApiLogs).toHaveBeenLastCalledWith({ limit: 50, offset: 0 });

    await user.click(screen.getByRole("combobox", { name: "筛选工具类型" }));
    await user.click(await screen.findByRole("option", { name: "妙想接口" }));

    await waitFor(() => {
      expect(api.listStockApiLogs).toHaveBeenLastCalledWith({
        limit: 50,
        offset: 0,
        tool_source: "mx",
      });
    });

    await user.click(screen.getByRole("combobox", { name: "筛选工具类型" }));
    await user.click(await screen.findByRole("option", { name: "聚合研判" }));

    await waitFor(() => {
      expect(api.listStockApiLogs).toHaveBeenLastCalledWith({
        limit: 50,
        offset: 0,
        tool_source: "aggregate",
      });
    });
  });

  it("loads call logs page by page", async () => {
    const user = userEvent.setup();
    api.getStockApiSettings.mockResolvedValue(stockApiSettings);
    api.listStockApiLogs
      .mockResolvedValueOnce({
        items: [
          {
            id: 51,
            tool_source: "aggregate",
            tool_id: "market_snapshot",
            tool_name: "行情查询",
            parameters: {},
            status: "success",
            duration_ms: 48,
            response_characters: 13_259,
            error_message: null,
            created_at: "2026-08-03T08:01:00Z",
          },
        ],
        total: 51,
        summary: {
          total_calls: 51,
          success_calls: 51,
          failed_calls: 0,
          average_duration_ms: 48,
        },
      })
      .mockResolvedValueOnce({
        items: [
          {
            id: 1,
            tool_source: "public",
            tool_id: "stock_quote",
            tool_name: "实时行情",
            parameters: { symbols: ["600519.SH"] },
            status: "success",
            duration_ms: 20,
            response_characters: 456,
            error_message: null,
            created_at: "2026-08-03T07:00:00Z",
          },
        ],
        total: 51,
        summary: {
          total_calls: 51,
          success_calls: 51,
          failed_calls: 0,
          average_duration_ms: 48,
        },
      });

    renderPage();
    await user.click(await screen.findByRole("tab", { name: "调用日志" }));

    expect(await screen.findByText("第 1 / 2 页")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "上一页" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "下一页" }));

    expect(await screen.findByText("第 2 / 2 页")).toBeInTheDocument();
    expect(await screen.findByText("实时行情")).toBeInTheDocument();
    expect(api.listStockApiLogs).toHaveBeenLastCalledWith({ limit: 50, offset: 50 });
    expect(screen.getByRole("button", { name: "下一页" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "上一页" })).toBeEnabled();
  });
});
