import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useOutletContext } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { LucideIcon } from "lucide-react";
import {
  ActivityIcon,
  CircleAlertIcon,
  CircleDollarSignIcon,
  KeyRoundIcon,
  RefreshCwIcon,
  ScaleIcon,
  WalletCardsIcon,
} from "lucide-react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { QueryErrorState, QueryLoadingState } from "@/components/query-state";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "@/components/ui/empty";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { accountKeys } from "@/features/dashboard/query-keys";
import { useRefreshAnimation } from "@/hooks/use-refresh-animation";
import {
  getAccountDashboard,
  getMarketDetails,
  getMarketIndices,
  getSettings,
  refreshAccountCache,
} from "@/lib/api";
import {
  formatCurrency,
  formatDateOnly,
  formatMonthDayTime,
  formatNumber,
  formatPercent,
  getErrorMessage,
} from "@/lib/format";
import { cn } from "@/lib/utils";

import { MarketOverview, MarketUpdateTime } from "./market-overview";

function formatNetValue(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return "--";
  return value.toFixed(3);
}

function formatPercentNoSign(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return "--";
  return `${(value * 100).toFixed(2)}%`;
}

function formatCount(value: number | null | undefined, suffix = "") {
  const formatted = formatNumber(value);
  return formatted === "--" ? formatted : `${formatted}${suffix}`;
}

function formatCompactStockName(stockName: string, maxLength = 4) {
  return Array.from(stockName).slice(0, maxLength).join("");
}

function translateOrderDirection(direction: string) {
  switch (direction.toUpperCase()) {
    case "BUY":
      return "买入";
    case "SELL":
      return "卖出";
    default:
      return direction;
  }
}

function translateOrderStatus(status: string) {
  switch (status.toUpperCase()) {
    case "PENDING":
      return "待成交";
    case "FILLED":
      return "已成交";
    case "PARTIAL":
      return "部分成交";
    case "PARTIAL_PENDING_CANCEL":
      return "部分成交待撤";
    case "PENDING_CANCEL":
      return "已报待撤";
    case "PARTIAL_CANCELLED":
      return "部撤";
    case "CANCELLED":
      return "已撤单";
    case "REJECTED":
      return "已废单";
    case "CANCEL_FAILED":
      return "撤单失败";
    case "UNKNOWN":
      return "未知";
    default:
      return status;
  }
}

function getChangeTone(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value) || value === 0) return undefined;
  return value > 0 ? "text-destructive" : "text-chart-2";
}

function getThresholdTone(value: number | null | undefined, threshold: number | null | undefined) {
  if (
    value === null ||
    value === undefined ||
    Number.isNaN(value) ||
    threshold === null ||
    threshold === undefined ||
    Number.isNaN(threshold)
  ) {
    return undefined;
  }
  return value >= threshold ? "text-destructive" : "text-chart-2";
}

type SummaryCardData = {
  title: string;
  value: string;
  valueLabel: string;
  icon: LucideIcon;
  valueClassName?: string | undefined;
  rows: { label: string; value: string; valueClassName?: string | undefined }[];
};

type DashboardLayoutContext = {
  setMainFixed: (fixed: boolean) => void;
};

type DashboardData = Awaited<ReturnType<typeof getAccountDashboard>>;
type MarketIndicesData = Awaited<ReturnType<typeof getMarketIndices>>;
type MarketDetailsData = Awaited<ReturnType<typeof getMarketDetails>>;
type MarketData = MarketIndicesData & MarketDetailsData;

const ACCOUNT_REFRESH_INTERVAL_MS = 10 * 60 * 1000;
const MARKET_REFRESH_INTERVAL_MS = 5 * 1000;
const MARKET_DETAILS_REFRESH_INTERVAL_MS = 60 * 1000;
const SHANGHAI_TIME_ZONE = "Asia/Shanghai";
const MARKET_SESSION_STARTS = [9 * 60 + 30, 13 * 60] as const;
const MARKET_SESSION_ENDS = [11 * 60 + 30, 15 * 60] as const;

type ShanghaiClock = { year: number; month: number; day: number; weekday: number; minutes: number };

function getShanghaiClock(now: Date): ShanghaiClock {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: SHANGHAI_TIME_ZONE,
    year: "numeric",
    month: "numeric",
    day: "numeric",
    weekday: "short",
    hour: "numeric",
    minute: "numeric",
    hourCycle: "h23",
  }).formatToParts(now);
  const value = (type: Intl.DateTimeFormatPartTypes) =>
    Number(parts.find((part) => part.type === type)?.value);
  const weekday = parts.find((part) => part.type === "weekday")?.value;
  return {
    year: value("year"),
    month: value("month"),
    day: value("day"),
    weekday: ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].indexOf(weekday ?? ""),
    minutes: value("hour") * 60 + value("minute"),
  };
}

function isWeekday(weekday: number) {
  return weekday >= 1 && weekday <= 5;
}

function nextShanghaiSessionStart(clock: ShanghaiClock) {
  const candidate = new Date(Date.UTC(clock.year, clock.month - 1, clock.day, 1, 30));
  if (isWeekday(clock.weekday) && clock.minutes < MARKET_SESSION_STARTS[0]) return candidate;
  if (isWeekday(clock.weekday) && clock.minutes < MARKET_SESSION_STARTS[1]) {
    candidate.setUTCHours(5, 0, 0, 0);
    return candidate;
  }
  candidate.setUTCDate(candidate.getUTCDate() + 1);
  while (!isWeekday(candidate.getUTCDay())) candidate.setUTCDate(candidate.getUTCDate() + 1);
  return candidate;
}

// The polling calculator is exported for deterministic boundary tests.
// eslint-disable-next-line react-refresh/only-export-components
export function getMarketRefreshInterval(now = new Date()) {
  const clock = getShanghaiClock(now);
  const inSession =
    isWeekday(clock.weekday) &&
    MARKET_SESSION_STARTS.some((start, index) => {
      const end = MARKET_SESSION_ENDS[index];
      return end !== undefined && clock.minutes >= start && clock.minutes < end;
    });
  if (inSession) return MARKET_REFRESH_INTERVAL_MS;
  return Math.max(1_000, nextShanghaiSessionStart(clock).getTime() - now.getTime());
}

export function DashboardPage() {
  const queryClient = useQueryClient();
  const settingsQuery = useQuery({ queryKey: ["settings"], queryFn: getSettings });
  const mxApiKeyConfigured = settingsQuery.data?.mx.api_key_configured === true;
  const dashboardQuery = useQuery({
    queryKey: accountKeys.dashboard(),
    queryFn: getAccountDashboard,
    enabled: mxApiKeyConfigured,
  });
  const marketIndicesQuery = useQuery({
    queryKey: ["market", "indices"],
    queryFn: getMarketIndices,
    refetchInterval: () => getMarketRefreshInterval(),
    refetchIntervalInBackground: true,
  });
  const marketDetailsQuery = useQuery({
    queryKey: ["market", "details"],
    queryFn: getMarketDetails,
    refetchInterval: () => {
      const interval = getMarketRefreshInterval();
      return interval === MARKET_REFRESH_INTERVAL_MS
        ? MARKET_DETAILS_REFRESH_INTERVAL_MS
        : interval;
    },
    refetchIntervalInBackground: true,
  });
  const marketData = useMemo<MarketData | undefined>(() => {
    if (!marketIndicesQuery.data || !marketDetailsQuery.data) return undefined;
    return {
      ...marketIndicesQuery.data,
      ...marketDetailsQuery.data,
      generated_at:
        marketIndicesQuery.data.generated_at > marketDetailsQuery.data.generated_at
          ? marketIndicesQuery.data.generated_at
          : marketDetailsQuery.data.generated_at,
      errors: [...marketIndicesQuery.data.errors, ...marketDetailsQuery.data.errors],
    };
  }, [marketDetailsQuery.data, marketIndicesQuery.data]);
  const { mutateAsync: refreshAccountMutation } = useMutation({
    mutationFn: refreshAccountCache,
  });
  const accountRefreshInFlight = useRef(false);
  const refreshAccountData = useCallback(
    async (notify: boolean) => {
      if (!mxApiKeyConfigured || accountRefreshInFlight.current) return;
      accountRefreshInFlight.current = true;
      try {
        const result = await refreshAccountMutation();
        await queryClient.invalidateQueries({ queryKey: ["account"] });
        if (notify) {
          if (result.status === "stale") toast.warning(result.message);
          else toast.success(result.message);
        }
      } catch (error) {
        if (notify) toast.error(getErrorMessage(error));
      } finally {
        accountRefreshInFlight.current = false;
      }
    },
    [mxApiKeyConfigured, queryClient, refreshAccountMutation],
  );
  useEffect(() => {
    if (!mxApiKeyConfigured) return;
    void refreshAccountData(false);
    const interval = window.setInterval(
      () => void refreshAccountData(false),
      ACCOUNT_REFRESH_INTERVAL_MS,
    );
    return () => window.clearInterval(interval);
  }, [mxApiKeyConfigured, refreshAccountData]);
  const [activeTab, setActiveTab] = useState("account");
  const layoutContext = useOutletContext<DashboardLayoutContext | undefined>();
  const setMainFixed = layoutContext?.setMainFixed;
  useEffect(() => {
    setMainFixed?.(activeTab === "account");
  }, [activeTab, setMainFixed]);
  const { isAnimating: refreshAnimating, start: startRefreshAnimation } = useRefreshAnimation();
  const refreshActive = refreshAnimating;
  const refreshDashboard = () =>
    startRefreshAnimation(async () => {
      await Promise.all([
        marketIndicesQuery.refetch(),
        marketDetailsQuery.refetch(),
        ...(mxApiKeyConfigured ? [refreshAccountData(true)] : []),
      ]);
    });
  const isAccountTab = activeTab === "account";

  if (settingsQuery.isPending) return <QueryLoadingState label="正在加载投资总览…" />;
  if (settingsQuery.isError) {
    return (
      <QueryErrorState
        error={settingsQuery.error}
        title="妙想设置加载失败"
        onRetry={() => void settingsQuery.refetch()}
      />
    );
  }

  return (
    <div className={isAccountTab ? "flex min-h-0 flex-1 flex-col gap-4" : "space-y-4"}>
      <div className="mb-2 flex items-center justify-between space-y-2">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">投资总览</h1>
          <p className="text-muted-foreground text-sm">聚焦账户状态、最近运行与组合变化</p>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className={cn(refreshActive && "cursor-wait")}
          disabled={refreshActive}
          aria-busy={refreshActive}
          title="刷新投资总览数据"
          onClick={refreshDashboard}
        >
          <RefreshCwIcon
            className={cn(
              "size-4 transition-transform duration-500",
              refreshActive && "animate-spin",
            )}
          />
          刷新数据
        </Button>
      </div>

      <Tabs
        value={activeTab}
        onValueChange={setActiveTab}
        className={isAccountTab ? "min-h-0 flex-1" : undefined}
      >
        <div className="flex min-w-0 items-end justify-between gap-3">
          <TabsList>
            <TabsTrigger value="account">账户总览</TabsTrigger>
            <TabsTrigger value="market">行情总览</TabsTrigger>
          </TabsList>
          {activeTab === "market" ? (
            <MarketUpdateTime data={marketData} />
          ) : (
            <AccountUpdateTime data={dashboardQuery.data} />
          )}
        </div>

        <TabsContent
          value="account"
          className={isAccountTab ? "flex min-h-0 flex-1 flex-col gap-4" : "space-y-4"}
        >
          {!mxApiKeyConfigured ? (
            <AccountKeyPrompt />
          ) : dashboardQuery.isPending ? (
            <QueryLoadingState label="正在加载账户总览…" />
          ) : dashboardQuery.isError && !dashboardQuery.data ? (
            <QueryErrorState
              error={dashboardQuery.error}
              title="账户总览加载失败"
              onRetry={() => void dashboardQuery.refetch()}
            />
          ) : dashboardQuery.data ? (
            <AccountOverview data={dashboardQuery.data} query={dashboardQuery} />
          ) : null}
        </TabsContent>

        <TabsContent value="market" className="space-y-4">
          <MarketOverview
            data={marketData}
            isLoading={marketIndicesQuery.isPending || marketDetailsQuery.isPending}
            error={marketIndicesQuery.error ?? marketDetailsQuery.error}
            onRefresh={() =>
              void Promise.all([marketIndicesQuery.refetch(), marketDetailsQuery.refetch()])
            }
          />
        </TabsContent>
      </Tabs>
    </div>
  );
}

function AccountUpdateTime({ data }: { data: DashboardData | undefined }) {
  return (
    <span className="text-muted-foreground text-xs tabular-nums">
      最近刷新：{formatMonthDayTime(data?.overview.captured_at)}
    </span>
  );
}

function AccountKeyPrompt() {
  return (
    <div
      role="status"
      className="border-border/60 bg-muted/20 flex flex-col gap-4 rounded-lg border px-4 py-4 sm:flex-row sm:items-center sm:justify-between"
    >
      <div className="flex min-w-0 items-start gap-3">
        <span className="inline-flex size-9 shrink-0 items-center justify-center rounded-md bg-amber-500/10 text-amber-700 dark:text-amber-300">
          <CircleAlertIcon className="size-4" />
        </span>
        <div className="min-w-0">
          <p className="text-sm font-medium">尚未配置妙想 API 密钥</p>
          <p className="text-muted-foreground mt-1 text-sm">
            配置后即可读取模拟组合的账户资产、持仓和委托数据。
          </p>
        </div>
      </div>
      <Button asChild className="self-start sm:self-auto">
        <Link to="/settings">
          <KeyRoundIcon />
          配置密钥
        </Link>
      </Button>
    </div>
  );
}

function AccountOverview({
  data,
  query,
}: {
  data: DashboardData;
  query: { isError: boolean; error: unknown; refetch: () => Promise<unknown> };
}) {
  const topPositions = useMemo(
    () => [...data.positions].sort((left, right) => right.market_value - left.market_value),
    [data.positions],
  );
  const recentOrders = data.orders.slice(0, 20);
  const summaryCards = useMemo<SummaryCardData[]>(() => {
    const overview = data.overview;
    const initialCapital = overview.initial_capital ?? null;
    const safeInitialCapital = initialCapital && initialCapital > 0 ? initialCapital : null;
    return [
      {
        title: "账户状态",
        value: formatCount(overview.operation_days),
        valueLabel: "运行天数",
        icon: ActivityIcon,
        rows: [
          { label: "开户日期", value: formatDateOnly(overview.open_date) },
          { label: "可用资金", value: formatCurrency(overview.available_cash) },
        ],
      },
      {
        title: "资金规模",
        value: formatCurrency(overview.total_asset),
        valueLabel: "账户总资产",
        icon: CircleDollarSignIcon,
        valueClassName: getThresholdTone(overview.total_asset, safeInitialCapital),
        rows: [
          { label: "持仓市值", value: formatCurrency(overview.market_value) },
          { label: "仓位比例", value: formatPercentNoSign(overview.position_ratio) },
        ],
      },
      {
        title: "累计表现",
        value: formatCurrency(overview.total_profit),
        valueLabel: "总收益金额",
        icon: ScaleIcon,
        valueClassName: getChangeTone(overview.total_profit),
        rows: [
          {
            label: "总收益率",
            value: formatPercent(overview.total_return_rate),
            valueClassName: getChangeTone(overview.total_profit),
          },
          {
            label: "当前净值",
            value: formatNetValue(overview.current_net_value),
            valueClassName: getThresholdTone(overview.current_net_value, 1),
          },
        ],
      },
      {
        title: `今日表现${overview.performance_date ? ` (${overview.performance_date})` : ""}`,
        value: formatCurrency(overview.daily_profit),
        valueLabel: "当日盈亏金额",
        icon: WalletCardsIcon,
        valueClassName: getChangeTone(overview.daily_profit),
        rows: [
          {
            label: "当日收益率",
            value: formatPercent(overview.daily_return_rate),
            valueClassName: getChangeTone(overview.daily_profit),
          },
          { label: "今日交易次数", value: formatCount(overview.today_trade_count, " 次") },
        ],
      },
    ];
  }, [data.overview]);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-4">
      <div className="grid shrink-0 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {summaryCards.map((card) => (
          <Card key={card.title}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">{card.title}</CardTitle>
              <card.icon className="text-muted-foreground h-4 w-4" />
            </CardHeader>
            <CardContent>
              <div className={cn("text-2xl font-bold", card.valueClassName)}>{card.value}</div>
              <p className="text-muted-foreground text-xs">{card.valueLabel}</p>
              <div className="mt-4 space-y-2 border-t pt-3">
                {card.rows.map((row) => (
                  <div key={row.label} className="flex items-center justify-between gap-2 text-xs">
                    <span className="text-muted-foreground">{row.label}</span>
                    <span className={cn("font-medium", row.valueClassName)}>{row.value}</span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {query.isError ? (
        <div
          role="alert"
          className="rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-800 dark:text-amber-200"
        >
          正在显示上次成功加载的数据；后台刷新失败：{getErrorMessage(query.error)}
        </div>
      ) : null}

      <div className="grid min-h-0 min-w-0 flex-1 gap-4 xl:grid-cols-2">
        <PositionTable positions={topPositions} />
        <OrderTable orders={recentOrders} />
      </div>
    </div>
  );
}

function PositionTable({ positions }: { positions: DashboardData["positions"] }) {
  return (
    <Card className="flex h-[415px] min-h-0 min-w-0 flex-col">
      <CardHeader>
        <CardTitle>持仓情况</CardTitle>
        <CardDescription>按市值排序显示持仓情况</CardDescription>
      </CardHeader>
      <CardContent className="flex min-h-0 flex-1 flex-col overflow-hidden">
        {positions.length === 0 ? (
          <Empty className="min-h-[250px] justify-center">
            <EmptyHeader>
              <EmptyTitle>暂无持仓</EmptyTitle>
              <EmptyDescription>账户持仓为空时，这里会显示空态</EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <div className="min-h-0 min-w-0 flex-1 overflow-auto pe-1">
            <Table className="table-fixed" containerClassName="overflow-visible">
              <TableHeader className="bg-card sticky top-0 z-10">
                <TableRow className="hover:bg-transparent">
                  <TableHead className="w-[18%] px-1.5 text-center">股票</TableHead>
                  <TableHead className="w-[14%] px-1.5 text-center">数量</TableHead>
                  <TableHead className="w-[15%] px-1.5 text-center">现价</TableHead>
                  <TableHead className="w-[15%] px-1.5 text-center">盈亏比</TableHead>
                  <TableHead className="w-[17%] px-1.5 text-center">市值</TableHead>
                  <TableHead className="w-[21%] px-1.5 text-center">当日盈亏</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {positions.map((position) => (
                  <TableRow key={position.symbol} className="h-[53.333px]">
                    <TableCell className="px-1.5 py-1 text-center">
                      <div className="flex flex-col items-center gap-0">
                        <span className="font-medium" title={position.stock_name}>
                          {formatCompactStockName(position.stock_name, 5)}
                        </span>
                        <span className="text-muted-foreground text-xs">{position.symbol}</span>
                      </div>
                    </TableCell>
                    <TableCell className="px-1.5 py-1 text-center tabular-nums">
                      {formatNumber(position.quantity)}
                    </TableCell>
                    <TableCell className="px-1.5 py-1 text-center tabular-nums">
                      {formatCurrency(position.current_price)}
                    </TableCell>
                    <TableCell
                      className={cn(
                        "px-1.5 py-1 text-center tabular-nums",
                        getChangeTone(position.profit_ratio),
                      )}
                    >
                      {formatPercent(position.profit_ratio)}
                    </TableCell>
                    <TableCell className="px-1.5 py-1 text-center tabular-nums">
                      {formatCurrency(position.market_value)}
                    </TableCell>
                    <TableCell
                      className={cn(
                        "px-1.5 py-1 text-center tabular-nums",
                        getChangeTone(position.day_profit),
                      )}
                    >
                      {formatCurrency(position.day_profit)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function OrderTable({ orders }: { orders: DashboardData["orders"] }) {
  return (
    <Card className="flex h-[415px] min-h-0 min-w-0 flex-col">
      <CardHeader>
        <CardTitle>委托情况</CardTitle>
        <CardDescription>来自模拟交易的委托情况</CardDescription>
      </CardHeader>
      <CardContent className="flex min-h-0 flex-1 flex-col overflow-hidden px-3">
        {orders.length === 0 ? (
          <Empty className="min-h-[250px] justify-center">
            <EmptyHeader>
              <EmptyTitle>暂无委托记录</EmptyTitle>
              <EmptyDescription>委托发生后，这里会展示最近动作</EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <div className="min-h-0 min-w-0 flex-1 overflow-auto pe-1">
            <Table className="table-fixed" containerClassName="overflow-visible">
              <TableHeader className="bg-card sticky top-0 z-10">
                <TableRow className="hover:bg-transparent">
                  <TableHead className="w-[20%] text-center whitespace-nowrap">委托时间</TableHead>
                  <TableHead className="w-[22%] text-center whitespace-nowrap">股票名称</TableHead>
                  <TableHead className="w-[12%] text-center whitespace-nowrap">方向</TableHead>
                  <TableHead className="w-[16%] text-center whitespace-nowrap">成交数量</TableHead>
                  <TableHead className="w-[14%] text-center whitespace-nowrap">成交价</TableHead>
                  <TableHead className="w-[16%] text-center whitespace-nowrap">状态</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {orders.map((order) => (
                  <TableRow key={order.order_id} className="h-[53.333px]">
                    <TableCell className="py-1 text-center tabular-nums">
                      {formatMonthDayTime(order.submitted_at ?? order.updated_at)}
                    </TableCell>
                    <TableCell className="py-1 text-center">
                      <div className="flex flex-col items-center gap-0.5">
                        <span
                          className="max-w-[4em] overflow-hidden font-medium whitespace-nowrap"
                          title={order.stock_name}
                          aria-label={order.stock_name}
                        >
                          {formatCompactStockName(order.stock_name)}
                        </span>
                        <span className="text-muted-foreground text-xs">{order.symbol}</span>
                      </div>
                    </TableCell>
                    <TableCell className="py-1 text-center">
                      {translateOrderDirection(order.direction)}
                    </TableCell>
                    <TableCell className="py-1 text-center tabular-nums">
                      {formatNumber(order.filled_quantity)}
                    </TableCell>
                    <TableCell className="py-1 text-center tabular-nums">
                      {formatCurrency(order.filled_price)}
                    </TableCell>
                    <TableCell className="py-1 text-center">
                      {translateOrderStatus(order.status)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
