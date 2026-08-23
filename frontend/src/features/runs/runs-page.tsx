import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  CircleAlertIcon,
  CircleCheckIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  CircleIcon,
  ClockIcon,
  LoaderCircleIcon,
  MinusIcon,
  Trash2Icon,
} from "lucide-react";
import { toast } from "sonner";

import { deleteRun, listRuns } from "@/lib/api";
import { formatMonthDayTime, formatRunDuration, getErrorMessage } from "@/lib/format";
import { RunWorkbenchPanel } from "@/features/runs/components/run-workbench";
import { formatStageState } from "@/lib/pipeline-stages";
import { RunStartButton } from "@/features/runs/components/run-start-button";
import { Button } from "@/components/ui/button";
import { QueryErrorState, QueryLoadingState } from "@/components/query-state";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Card, CardContent } from "@/components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "@/components/ui/empty";
import { cn } from "@/lib/utils";
import type { RunSummary } from "@/lib/api-types";
import { runKeys } from "@/features/runs/query-keys";

function RunCardStatusIcon({ status }: { status: string }) {
  switch (status) {
    case "RUNNING":
      return <LoaderCircleIcon className="size-2.5 animate-spin text-emerald-600" aria-hidden />;
    case "FAILED":
      return <CircleAlertIcon className="text-destructive size-2.5" aria-hidden />;
    case "ABORTED":
      return <MinusIcon className="text-destructive size-2.5" aria-hidden />;
    case "COMPLETED":
      return <CircleCheckIcon className="size-2.5 text-emerald-600" aria-hidden />;
    default:
      return <CircleIcon className="text-destructive size-2.5" aria-hidden />;
  }
}

function runCardDetail(run: RunSummary, isStopping: boolean, duration: string) {
  if (run.status === "RUNNING") {
    return isStopping ? "停止中…" : `${formatStageState(run.current_state)} · ${duration}`;
  }
  if (run.status === "FAILED") return `运行失败 · ${duration}`;
  if (run.status === "ABORTED") return `已中止 · ${duration}`;
  if (run.status === "COMPLETED") return `已完成 · ${duration}`;
  return `${run.status} · ${duration}`;
}

const compactMetricFormatter = new Intl.NumberFormat("zh-CN", {
  notation: "compact",
  maximumFractionDigits: 1,
});

function formatMetricCount(value: number | null | undefined) {
  const normalizedValue = typeof value === "number" && Number.isFinite(value) ? value : 0;
  return compactMetricFormatter.format(Math.max(0, normalizedValue));
}

const RUNS_PAGE_SIZE = 20;
const RUNS_PAGE_FETCH_SIZE = RUNS_PAGE_SIZE + 1;
type RunsPageScrollTarget = "start" | "end";

function upsertRunSummary(previous: RunSummary[] | undefined, run: RunSummary) {
  const existing = previous ?? [];
  return [run, ...existing.filter((item) => item.run_id !== run.run_id)].sort(
    (left, right) => right.run_id - left.run_id,
  );
}

export function RunsPage() {
  const queryClient = useQueryClient();
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null);
  const [stoppingRunId, setStoppingRunId] = useState<number | null>(null);
  const [runsPage, setRunsPage] = useState(0);
  const runListRef = useRef<HTMLDivElement>(null);
  const pendingScrollTargetRef = useRef<RunsPageScrollTarget | null>(null);

  const runsQuery = useQuery({
    queryKey: runKeys.list(runsPage),
    queryFn: () => listRuns(RUNS_PAGE_FETCH_SIZE, runsPage * RUNS_PAGE_SIZE),
    // Keep discovering runs started by schedules or other sessions. The active
    // detail itself streams through SSE, so this interval need not be tight.
    refetchInterval: 15_000,
  });
  // Page 0 already includes the active run. Only later pages need a separate
  // first-page scan, preventing duplicate offset=0 requests on the common view.
  const activeRunsQuery = useQuery({
    queryKey: runKeys.active(),
    queryFn: () => listRuns(RUNS_PAGE_FETCH_SIZE, 0),
    enabled: runsPage > 0,
    refetchInterval: runsPage > 0 ? 15_000 : false,
  });

  const runsPageData = useMemo(() => runsQuery.data ?? [], [runsQuery.data]);
  const runs = useMemo(() => runsPageData.slice(0, RUNS_PAGE_SIZE), [runsPageData]);
  const hasNextRunsPage = runsPageData.length > RUNS_PAGE_SIZE;
  const latestRuns = useMemo(
    () => (runsPage === 0 ? runsPageData : (activeRunsQuery.data ?? [])),
    [activeRunsQuery.data, runsPage, runsPageData],
  );
  const hasVisibleRunningRun = useMemo(() => runs.some((run) => run.status === "RUNNING"), [runs]);
  const [now, setNow] = useState(() => new Date());

  useLayoutEffect(() => {
    const container = runListRef.current;
    const target = pendingScrollTargetRef.current;
    if (container === null || target === null || runs.length === 0) {
      return;
    }

    pendingScrollTargetRef.current = null;
    container.scrollLeft =
      target === "end" ? Math.max(0, container.scrollWidth - container.clientWidth) : 0;
    const edgeRun = target === "end" ? runs.at(-1) : runs[0];
    setSelectedRunId(edgeRun?.run_id ?? null);
  }, [runs, runsPage]);

  useEffect(() => {
    const container = runListRef.current;
    if (container === null) {
      return;
    }

    const handleWheel = (event: WheelEvent) => {
      if (Math.abs(event.deltaY) <= Math.abs(event.deltaX)) {
        return;
      }

      const maxScrollLeft = container.scrollWidth - container.clientWidth;
      const nextScrollLeft = Math.max(
        0,
        Math.min(maxScrollLeft, container.scrollLeft + event.deltaY),
      );

      if (nextScrollLeft !== container.scrollLeft) {
        event.preventDefault();
        container.scrollLeft = nextScrollLeft;
      }
    };

    container.addEventListener("wheel", handleWheel, { passive: false });
    return () => container.removeEventListener("wheel", handleWheel);
  }, [runs.length, runsPage]);

  useEffect(() => {
    if (!hasVisibleRunningRun) {
      return;
    }

    const timer = window.setInterval(() => setNow(new Date()), 1_000);
    return () => window.clearInterval(timer);
  }, [hasVisibleRunningRun]);

  const effectiveSelectedRunId = useMemo(() => {
    if (runs.length === 0) {
      return null;
    }

    const stillVisible = selectedRunId !== null && runs.some((run) => run.run_id === selectedRunId);
    return stillVisible ? selectedRunId : (runs[0]?.run_id ?? null);
  }, [runs, selectedRunId]);
  const selectedRun = useMemo(
    () => runs.find((run) => run.run_id === effectiveSelectedRunId) ?? null,
    [effectiveSelectedRunId, runs],
  );
  const runningRun = useMemo(
    () => latestRuns.find((run) => run.status === "RUNNING") ?? null,
    [latestRuns],
  );
  const hasRunningRun = runningRun !== null;
  const activeRunDiscoveryUnavailable =
    runsPage > 0 && !activeRunsQuery.data && (activeRunsQuery.isPending || activeRunsQuery.isError);
  const activeRunDiscoveryFailed = activeRunDiscoveryUnavailable && activeRunsQuery.isError;
  const effectiveStoppingRunId = useMemo(() => {
    if (stoppingRunId === null) {
      return null;
    }

    return latestRuns.some((run) => run.run_id === stoppingRunId && run.status === "RUNNING")
      ? stoppingRunId
      : null;
  }, [latestRuns, stoppingRunId]);

  const deleteMutation = useMutation({
    mutationFn: deleteRun,
    onSuccess: async (_, deletedRunId) => {
      toast.success("运行记录已删除");
      setSelectedRunId((current) => (current === deletedRunId ? null : current));
      if (runsPage > 0 && runs.length === 1) {
        pendingScrollTargetRef.current = "end";
        setRunsPage((current) => Math.max(0, current - 1));
      }
      queryClient.removeQueries({ queryKey: runKeys.detail(deletedRunId) });
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: runKeys.all }),
        queryClient.invalidateQueries({ queryKey: runKeys.active() }),
      ]);
    },
    onError: (error) => toast.error(getErrorMessage(error)),
  });

  if (runsQuery.isPending) {
    return <QueryLoadingState label="正在加载运行记录…" />;
  }

  if (runsQuery.isError && !runsQuery.data) {
    return (
      <QueryErrorState
        error={runsQuery.error}
        title="运行记录加载失败"
        onRetry={() => void runsQuery.refetch()}
      />
    );
  }

  return (
    <div className="text-foreground flex min-h-0 flex-1 flex-col space-y-4 font-sans">
      {runsQuery.isError ? (
        <div
          role="alert"
          className="rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-800"
        >
          正在显示上次成功加载的运行记录；后台刷新失败：{getErrorMessage(runsQuery.error)}
        </div>
      ) : null}
      {activeRunDiscoveryUnavailable ? (
        <div
          role={activeRunDiscoveryFailed ? "alert" : "status"}
          aria-live="polite"
          className="rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-800"
        >
          {activeRunDiscoveryFailed
            ? "无法确认是否已有运行中的任务。为避免重复执行，手动运行暂时不可用；请重新加载页面后再试。"
            : "正在确认是否已有运行中的任务；确认完成前不能发起新的手动运行。"}
        </div>
      ) : null}
      <div className="mb-2 flex items-center justify-between space-y-2">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">任务运行</h1>
          <p className="text-muted-foreground text-sm">任务运行总览</p>
        </div>
        <div className="flex items-center gap-2">
          <RunStartButton
            triggerLabel="手动运行"
            disabled={hasRunningRun || activeRunDiscoveryUnavailable}
            runningRunId={runningRun?.run_id ?? null}
            isStopping={
              effectiveStoppingRunId !== null && effectiveStoppingRunId === runningRun?.run_id
            }
            onStartRequested={() => setStoppingRunId(null)}
            onStopRequested={setStoppingRunId}
            onStopFailed={(runId) => {
              setStoppingRunId((current) => (current === runId ? null : current));
            }}
            onStarted={(run) => {
              setStoppingRunId(null);
              pendingScrollTargetRef.current = "start";
              setRunsPage(0);
              setSelectedRunId(run.run_id);
              queryClient.setQueryData<RunSummary[]>(runKeys.active(), (previous) =>
                upsertRunSummary(previous, run),
              );
              queryClient.setQueryData<RunSummary[]>(["runs", "list", 0], (previous) =>
                upsertRunSummary(previous, run).slice(0, RUNS_PAGE_FETCH_SIZE),
              );
            }}
          />
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button
                variant="destructive"
                disabled={
                  selectedRun === null ||
                  selectedRun.status === "RUNNING" ||
                  deleteMutation.isPending
                }
              >
                <Trash2Icon data-icon="inline-start" />
                删除记录
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent size="sm">
              <AlertDialogHeader>
                <AlertDialogTitle>删除运行记录</AlertDialogTitle>
                <AlertDialogDescription>
                  删除后将一并移除该次运行的时间线事件，无法恢复
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>取消</AlertDialogCancel>
                <AlertDialogAction
                  variant="destructive"
                  onClick={() => {
                    if (selectedRun !== null) {
                      deleteMutation.mutate(selectedRun.run_id);
                    }
                  }}
                >
                  确认删除
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </div>
      </div>

      <Card className="border-border/75 bg-card/90 h-[124px] gap-0 py-0 shadow-sm">
        <CardContent className="flex h-full items-center px-3 py-3">
          {runs.length === 0 ? (
            <Empty className="h-full min-h-0 justify-center py-0">
              <EmptyHeader>
                <EmptyTitle>暂无运行记录</EmptyTitle>
                <EmptyDescription>
                  你可以切换到其他日期查看历史运行，或直接发起一次手动运行
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <>
              <div
                key={runsPage}
                ref={runListRef}
                data-testid="runs-scroll-list"
                className="flex min-w-0 flex-1 items-center gap-1.5 overflow-x-auto overflow-y-hidden p-1 pb-2 [scrollbar-width:thin]"
              >
                {runsPage > 0 ? (
                  <Button
                    type="button"
                    variant="outline"
                    size="icon"
                    className="h-[74px] w-9 shrink-0 rounded-md"
                    disabled={runsQuery.isFetching}
                    onClick={() => {
                      pendingScrollTargetRef.current = "end";
                      setRunsPage((current) => Math.max(0, current - 1));
                    }}
                    aria-label="上一页任务"
                    title={`上一页（第 ${runsPage} 页）`}
                  >
                    <ChevronLeftIcon className="size-4" />
                  </Button>
                ) : null}
                {runs.map((run) => {
                  const isRunning = run.status === "RUNNING";
                  const isStopping = isRunning && run.run_id === effectiveStoppingRunId;
                  const isSelected = run.run_id === effectiveSelectedRunId;
                  const duration = formatRunDuration(run.started_at, run.completed_at, now);
                  const detail = runCardDetail(run, isStopping, duration);
                  const startedAt = formatMonthDayTime(run.started_at);
                  const tokenTotal = formatMetricCount(run.total_tokens);
                  const tradeCount = formatMetricCount(run.trade_count);
                  const tradeLabel = `交易 ${tradeCount} 次`;

                  return (
                    <button
                      key={run.run_id}
                      type="button"
                      onClick={() => setSelectedRunId(run.run_id)}
                      aria-label={
                        isRunning
                          ? `查看运行 ${run.task_id} 的实时进度`
                          : `查看运行 ${run.task_id} 的执行记录`
                      }
                      title={`${startedAt} · ${detail}`}
                      aria-pressed={isSelected}
                      className="group focus-visible:ring-ring/50 w-[188px] shrink-0 rounded-md text-start outline-none focus-visible:ring-[3px]"
                    >
                      <span
                        className={cn(
                          "border-input flex h-[74px] flex-col rounded-md border bg-transparent px-3 py-2 shadow-xs transition-[background-color,border-color,box-shadow]",
                          isRunning
                            ? cn(
                                "border-sky-500/35 bg-sky-500/[0.06] group-hover:border-sky-500/55 group-hover:bg-sky-500/[0.1]",
                                isSelected && "ring-[3px] ring-sky-500/20",
                              )
                            : isSelected
                              ? "border-ring ring-ring/35 bg-transparent ring-[3px]"
                              : "group-hover:border-ring/60 group-hover:bg-muted/20",
                        )}
                      >
                        <span className="flex items-start justify-between gap-2">
                          <span className="min-w-0">
                            <span className="text-muted-foreground flex h-2.5 items-center gap-1 text-[8px] leading-none font-medium tracking-wide">
                              <ClockIcon className="size-2.5 shrink-0" aria-hidden />
                              <span>启动时间</span>
                            </span>
                            <span className="text-foreground mt-1 block truncate text-[12px] leading-none font-semibold tracking-tight tabular-nums">
                              {startedAt}
                            </span>
                          </span>
                          <span className="flex shrink-0 flex-col items-end">
                            <span className="text-muted-foreground flex h-2.5 items-center justify-end gap-1 text-[8px] leading-none font-medium tracking-wide">
                              <span>任务状态</span>
                              <RunCardStatusIcon status={run.status} />
                            </span>
                            <span className="text-foreground mt-1 text-[12px] leading-none font-semibold tracking-tight whitespace-nowrap tabular-nums">
                              {tradeLabel}
                            </span>
                          </span>
                        </span>

                        <span className="divide-border/55 border-border/45 mt-auto grid grid-cols-3 divide-x border-t pt-1.5">
                          <span className="flex min-w-0 flex-col pe-2">
                            <span className="text-muted-foreground text-[8px] leading-none">
                              工具调用
                            </span>
                            <span className="text-foreground mt-1 text-[10px] leading-none font-semibold tabular-nums">
                              {formatMetricCount(run.tool_calls_count)}
                            </span>
                          </span>
                          <span className="flex min-w-0 flex-col px-2">
                            <span className="text-muted-foreground text-[8px] leading-none">
                              深度思考
                            </span>
                            <span className="text-foreground mt-1 text-[10px] leading-none font-semibold tabular-nums">
                              {formatMetricCount(run.thinking_count)}
                            </span>
                          </span>
                          <span
                            className="flex min-w-0 flex-col ps-2"
                            title={`估算 Token 总数：${run.total_tokens.toLocaleString("zh-CN")}`}
                          >
                            <span className="text-muted-foreground text-[8px] leading-none">
                              Token 消耗
                            </span>
                            <span className="text-foreground mt-1 truncate text-[10px] leading-none font-semibold tabular-nums">
                              {tokenTotal}
                            </span>
                          </span>
                        </span>
                      </span>
                    </button>
                  );
                })}
                {hasNextRunsPage ? (
                  <Button
                    type="button"
                    variant="outline"
                    size="icon"
                    className="h-[74px] w-9 shrink-0 rounded-md"
                    disabled={runsQuery.isFetching}
                    onClick={() => {
                      pendingScrollTargetRef.current = "start";
                      setRunsPage((current) => current + 1);
                    }}
                    aria-label="下一页任务"
                    title={`下一页（第 ${runsPage + 2} 页）`}
                  >
                    <ChevronRightIcon className="size-4" />
                  </Button>
                ) : null}
              </div>
            </>
          )}
        </CardContent>
      </Card>

      <div className="flex min-h-0 flex-1 flex-col">
        <RunWorkbenchPanel runId={effectiveSelectedRunId} now={now} />
      </div>
    </div>
  );
}
