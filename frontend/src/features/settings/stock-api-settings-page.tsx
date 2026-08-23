import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  ChevronLeftIcon,
  ChevronRightIcon,
  LayersIcon,
  ListTreeIcon,
  RefreshCwIcon,
  WrenchIcon,
} from "lucide-react";

import { QueryErrorState, QueryLoadingState } from "@/components/query-state";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { stockApiKeys } from "@/features/settings/query-keys";
import { getStockApiSettings, listStockApiLogs } from "@/lib/api";
import type {
  StockApiLogToolSource,
  StockApiPublicProvider,
  StockApiSettings,
} from "@/lib/api-types";
import {
  formatStockApiParameters,
  formatStockApiProvider,
  formatStockApiToolSource,
} from "@/lib/stock-api-format";
import { cn } from "@/lib/utils";

const STOCK_API_QUERY_KEY = stockApiKeys.settings;
const STOCK_API_LOGS_QUERY_KEY = (toolSource: StockApiLogToolSource | undefined, page: number) =>
  ["stock-api-logs", toolSource ?? "all", page] as const;
const STOCK_API_LOGS_PAGE_SIZE = 50;

type StockApiTabId = "directory" | "system" | "logs";
type StockApiLogToolSourceFilter = "all" | StockApiLogToolSource;

const stockApiNavigationItems = [
  {
    id: "directory",
    label: "数据工具",
    icon: LayersIcon,
    description: "查看数据工具目录。",
  },
  {
    id: "system",
    label: "系统工具",
    icon: WrenchIcon,
    description: "查看运行阶段使用的内置工具。",
  },
  {
    id: "logs",
    label: "调用日志",
    icon: ListTreeIcon,
    description: "查看 Agent 数据工具调用记录。",
  },
] as const;

const stockApiLogToolSources = [
  "public",
  "aggregate",
  "mx",
] as const satisfies readonly StockApiLogToolSource[];

function formatTimestamp(value: string) {
  return new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "short",
    timeStyle: "medium",
  }).format(new Date(value));
}

function StockApiDirectory({ settings }: { settings: StockApiSettings }) {
  const publicStock = settings.public_stock;

  return (
    <section className="grid gap-3 md:grid-cols-2" aria-label="数据工具">
      {publicStock.tools.map((item) => (
        <article
          key={item.tool_name}
          className="border-border/60 min-w-0 rounded-md border px-4 py-3"
        >
          <div className="flex flex-wrap items-center gap-1.5">
            <h3 className="text-sm font-semibold">{item.name}</h3>
            <StockApiProviderBadges providers={item.providers} label={`${item.name}数据来源`} />
          </div>
          <p className="text-muted-foreground mt-1 truncate text-sm leading-6" title={item.summary}>
            {item.summary}
          </p>
          <div className="mt-3 flex flex-wrap gap-1.5">
            {item.actions.map((action) => (
              <span
                key={action}
                className="bg-muted text-muted-foreground rounded-sm px-2 py-1 text-xs"
              >
                {action}
              </span>
            ))}
          </div>
        </article>
      ))}
      {settings.mx.interfaces.map((item) => (
        <article key={item.id} className="border-border/60 min-w-0 rounded-md border px-4 py-3">
          <div className="flex flex-wrap items-center gap-1.5">
            <h3 className="text-sm font-semibold">{item.name}</h3>
            <StockApiProviderBadges providers={["mx"]} label={`${item.name}数据来源`} />
          </div>
          <p className="text-muted-foreground mt-1 truncate text-sm leading-6" title={item.summary}>
            {item.summary}
          </p>
          <div className="mt-3 flex flex-wrap gap-1.5">
            {item.features.map((feature) => (
              <span
                key={feature}
                className="bg-muted text-muted-foreground rounded-sm px-2 py-1 text-xs"
              >
                {feature}
              </span>
            ))}
          </div>
        </article>
      ))}
    </section>
  );
}

const systemTools = [
  {
    id: "memory_read",
    name: "记忆查询",
    summary: "检索历史交易记忆，为当前判断提供可验证的参考。",
    actions: ["检索记忆", "查看详情", "查看版本"],
  },
  {
    id: "memory_write",
    name: "记忆写入",
    summary: "将本次运行中验证过的事实沉淀为可演化的长期记忆。",
    actions: ["创建记忆", "更新记忆", "替代记忆", "废弃记忆"],
  },
] as const;

function SystemToolsPanel() {
  return (
    <section className="grid gap-3 md:grid-cols-2" aria-label="系统工具">
      {systemTools.map((item) => (
        <article key={item.id} className="border-border/60 min-w-0 rounded-md border px-4 py-3">
          <div className="flex flex-wrap items-center gap-1.5">
            <h3 className="text-sm font-semibold">{item.name}</h3>
            <code className="text-muted-foreground text-xs">{item.id}</code>
          </div>
          <p className="text-muted-foreground mt-1 text-sm leading-6">{item.summary}</p>
          <div className="mt-3 flex flex-wrap gap-1.5">
            {item.actions.map((action) => (
              <span
                key={action}
                className="bg-muted text-muted-foreground rounded-sm px-2 py-1 text-xs"
              >
                {action}
              </span>
            ))}
          </div>
        </article>
      ))}
    </section>
  );
}

function StockApiProviderBadge({ provider }: { provider: StockApiPublicProvider }) {
  const providerClass: Record<StockApiPublicProvider, string> = {
    eastmoney: "border-orange-500/25 bg-orange-500/[0.08] text-orange-700 dark:text-orange-300",
    tencent: "border-cyan-500/25 bg-cyan-500/[0.08] text-cyan-700 dark:text-cyan-300",
    sina: "border-rose-500/25 bg-rose-500/[0.08] text-rose-700 dark:text-rose-300",
    mx: "border-violet-500/25 bg-violet-500/[0.08] text-violet-700 dark:text-violet-300",
  };

  return (
    <Badge
      variant="outline"
      className={cn("h-5 rounded-sm px-1.5 text-[11px] font-medium", providerClass[provider])}
    >
      {formatStockApiProvider(provider)}
    </Badge>
  );
}

function StockApiProviderBadges({
  providers,
  label,
}: {
  providers: readonly StockApiPublicProvider[];
  label: string;
}) {
  return (
    <div className="flex flex-wrap items-center gap-1" aria-label={label}>
      {providers.map((provider) => (
        <StockApiProviderBadge key={provider} provider={provider} />
      ))}
    </div>
  );
}

function StockApiToolSourceBadge({ source }: { source: StockApiLogToolSource }) {
  const sourceClass: Record<StockApiLogToolSource, string> = {
    public: "border-cyan-500/25 bg-cyan-500/[0.08] text-cyan-700 dark:text-cyan-300",
    aggregate: "border-emerald-500/25 bg-emerald-500/[0.08] text-emerald-700 dark:text-emerald-300",
    mx: "border-violet-500/25 bg-violet-500/[0.08] text-violet-700 dark:text-violet-300",
  };

  return (
    <Badge
      variant="outline"
      className={cn("h-5 rounded-sm px-1.5 text-[11px] font-medium", sourceClass[source])}
    >
      {formatStockApiToolSource(source)}
    </Badge>
  );
}

function StockApiLogsPanel() {
  const [toolSourceFilter, setToolSourceFilter] = useState<StockApiLogToolSourceFilter>("all");
  const [page, setPage] = useState(1);
  const toolSource = toolSourceFilter === "all" ? undefined : toolSourceFilter;
  const logsQuery = useQuery({
    queryKey: STOCK_API_LOGS_QUERY_KEY(toolSource, page),
    queryFn: () =>
      listStockApiLogs({
        limit: STOCK_API_LOGS_PAGE_SIZE,
        offset: (page - 1) * STOCK_API_LOGS_PAGE_SIZE,
        ...(toolSource === undefined ? {} : { tool_source: toolSource }),
      }),
  });
  const logs = logsQuery.data;
  const totalPages = Math.max(1, Math.ceil((logs?.total ?? 0) / STOCK_API_LOGS_PAGE_SIZE));

  return (
    <section className="space-y-4" aria-label="数据工具调用日志">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="min-h-5">
          {logs ? (
            <p className="text-muted-foreground text-xs">
              共 {logs.total.toLocaleString("zh-CN")} 条
            </p>
          ) : null}
        </div>
        <div className="flex items-center gap-1">
          <Select
            value={toolSourceFilter}
            onValueChange={(value) => {
              setToolSourceFilter(value as StockApiLogToolSourceFilter);
              setPage(1);
            }}
          >
            <SelectTrigger size="sm" aria-label="筛选工具类型" className="w-32">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">全部工具</SelectItem>
              {stockApiLogToolSources.map((item) => (
                <SelectItem key={item} value={item}>
                  {formatStockApiToolSource(item)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            title="刷新调用日志"
            aria-label="刷新调用日志"
            disabled={logsQuery.isFetching}
            onClick={() => void logsQuery.refetch()}
          >
            <RefreshCwIcon className={cn("size-4", logsQuery.isFetching && "animate-spin")} />
          </Button>
        </div>
      </div>
      {logsQuery.isError && !logs ? (
        <QueryErrorState
          title="调用日志加载失败"
          error={logsQuery.error}
          onRetry={() => void logsQuery.refetch()}
        />
      ) : logs ? (
        <>
          <div className="max-h-[360px] min-w-0 overflow-auto pe-1">
            <Table containerClassName="overflow-visible">
              <TableHeader className="bg-card sticky top-0 z-10">
                <TableRow className="hover:bg-transparent">
                  <TableHead className="text-center">时间</TableHead>
                  <TableHead className="text-center">工具类型</TableHead>
                  <TableHead className="text-center">工具</TableHead>
                  <TableHead className="text-center">参数</TableHead>
                  <TableHead className="text-center">结果</TableHead>
                  <TableHead className="text-center">返回字符</TableHead>
                  <TableHead className="text-center">耗时</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {logs.items.map((item) => (
                  <TableRow key={item.id} className="h-[53.333px]">
                    <TableCell className="py-1 text-center tabular-nums">
                      {formatTimestamp(item.created_at)}
                    </TableCell>
                    <TableCell className="py-1 text-center">
                      <StockApiToolSourceBadge source={item.tool_source} />
                    </TableCell>
                    <TableCell className="py-1 text-center">{item.tool_name}</TableCell>
                    <TableCell
                      className="max-w-[22rem] truncate py-1 text-center"
                      title={formatStockApiParameters(item.parameters)}
                    >
                      {formatStockApiParameters(item.parameters)}
                    </TableCell>
                    <TableCell className="py-1 text-center">
                      <Badge
                        variant="outline"
                        className={cn(
                          "h-5 rounded-sm px-1.5 text-[11px] font-medium",
                          item.status === "success"
                            ? "border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
                            : "border-destructive/25 bg-destructive/10 text-destructive",
                        )}
                      >
                        {item.status === "success" ? "成功" : "失败"}
                      </Badge>
                    </TableCell>
                    <TableCell className="py-1 text-center tabular-nums">
                      {item.response_characters === null
                        ? "-"
                        : item.response_characters.toLocaleString("zh-CN")}
                    </TableCell>
                    <TableCell className="py-1 text-center tabular-nums">
                      {item.duration_ms} ms
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            {logs.items.length === 0 ? (
              <p className="text-muted-foreground px-3 py-8 text-center text-sm">暂无调用记录</p>
            ) : null}
          </div>
          <div className="border-border/60 flex items-center justify-between border-t px-3 py-2">
            <p className="text-muted-foreground text-xs tabular-nums">
              第 {page.toLocaleString("zh-CN")} / {totalPages.toLocaleString("zh-CN")} 页
            </p>
            <div className="flex items-center gap-1">
              <Button
                type="button"
                variant="ghost"
                size="icon"
                title="上一页"
                aria-label="上一页"
                disabled={page <= 1 || logsQuery.isFetching}
                onClick={() => setPage((value) => Math.max(1, value - 1))}
              >
                <ChevronLeftIcon className="size-4" />
              </Button>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                title="下一页"
                aria-label="下一页"
                disabled={page >= totalPages || logsQuery.isFetching}
                onClick={() => setPage((value) => Math.min(totalPages, value + 1))}
              >
                <ChevronRightIcon className="size-4" />
              </Button>
            </div>
          </div>
        </>
      ) : (
        <div className="text-muted-foreground py-8 text-center text-sm">正在加载调用日志…</div>
      )}
    </section>
  );
}

export function StockApiSettingsPage() {
  const [activeTab, setActiveTab] = useState<StockApiTabId>("directory");
  const settingsQuery = useQuery({
    queryKey: STOCK_API_QUERY_KEY,
    queryFn: getStockApiSettings,
  });
  const settings = settingsQuery.data;

  if (settingsQuery.isLoading) {
    return <QueryLoadingState label="正在加载工具管理…" />;
  }
  if (settingsQuery.isError && !settings) {
    return (
      <QueryErrorState
        title="工具管理加载失败"
        error={settingsQuery.error}
        onRetry={() => void settingsQuery.refetch()}
      />
    );
  }
  if (!settings) return null;

  const activeNavigationItem =
    stockApiNavigationItems.find((item) => item.id === activeTab) ?? stockApiNavigationItems[0];
  const ActiveNavigationIcon = activeNavigationItem.icon;

  return (
    <section className="h-full min-h-0 overflow-hidden" aria-label="工具管理内容">
      <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[11rem_minmax(0,1fr)] xl:gap-12">
        <aside className="top-0 h-fit xl:sticky">
          <nav className="space-y-1 p-1" aria-label="工具管理导航" role="tablist">
            {stockApiNavigationItems.map((item) => {
              const Icon = item.icon;
              const selected = item.id === activeTab;
              return (
                <Button
                  key={item.id}
                  id={`stock-api-tab-${item.id}`}
                  type="button"
                  role="tab"
                  aria-selected={selected}
                  aria-controls="stock-api-settings-panel"
                  variant="ghost"
                  className={cn(
                    "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground active:bg-sidebar-accent active:text-sidebar-accent-foreground dark:hover:bg-sidebar-accent h-9 w-full justify-start gap-2 rounded-md px-3",
                    selected && "bg-sidebar-accent text-sidebar-accent-foreground",
                  )}
                  onClick={() => setActiveTab(item.id)}
                >
                  <Icon className="size-4 shrink-0" />
                  <span>{item.label}</span>
                </Button>
              );
            })}
          </nav>
        </aside>

        <Card
          id="stock-api-settings-panel"
          role="tabpanel"
          aria-labelledby={`stock-api-tab-${activeTab}`}
          className="h-full min-h-0 gap-2 overflow-hidden py-4"
        >
          <CardHeader className="bg-background flex-none !gap-1.5 border-b !pb-1">
            <div className="flex items-start gap-3">
              <div key={activeTab} className="text-primary pt-0.5">
                <ActiveNavigationIcon className="size-5" />
              </div>
              <div key={`${activeTab}-text`} className="min-w-0">
                <CardTitle>{activeNavigationItem.label}</CardTitle>
                <CardDescription className="mt-1">
                  {activeNavigationItem.description}
                </CardDescription>
              </div>
            </div>
          </CardHeader>

          <CardContent key={activeTab} className="min-h-0 flex-1 overflow-y-auto pt-2 pb-6">
            {activeTab === "directory" ? (
              <StockApiDirectory settings={settings} />
            ) : activeTab === "system" ? (
              <SystemToolsPanel />
            ) : (
              <StockApiLogsPanel />
            )}
          </CardContent>
        </Card>
      </div>
    </section>
  );
}
