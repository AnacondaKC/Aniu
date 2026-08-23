import { useEffect, useState } from "react";
import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import {
  ActivityIcon,
  ArchiveIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  ClockIcon,
  MoonStarIcon,
  PlayIcon,
  PlusIcon,
  RefreshCwIcon,
  SearchIcon,
  Trash2Icon,
  XIcon,
} from "lucide-react";
import { toast } from "sonner";

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
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "@/components/ui/empty";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import {
  createMemory,
  deleteMemory,
  deleteMemoryDream,
  getMemoryDream,
  getMemoryOverview,
  listMemoryDreams,
  runMemoryDream,
  updateMemory,
} from "@/lib/api";
import type { MemoryActivity, MemoryDream, MemoryItem } from "@/lib/api-types";
import { formatMonthDayTime, getErrorMessage } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useRefreshAnimation } from "@/hooks/use-refresh-animation";
import { memoryKeys } from "@/features/memories/query-keys";

const operationLabels: Record<string, string> = {
  read: "记忆读取",
  create: "记忆写入",
  update: "记忆修改",
  delete: "记忆删除",
};

const DREAM_PAGE_SIZE = 10;
const ACTIVITY_PAGE_SIZE = 20;
const MEMORY_PAGE_SIZE = 10;
const MEMORY_TAB_CARD_CLASS = "h-[415px] min-h-0 overflow-hidden";
const NESTED_LIST_ITEM_CLASS =
  "border-border/40 bg-muted/20 flex items-center gap-3 rounded-md border px-4 py-3";

const operationBadgeClass: Record<string, string> = {
  read: "border-blue-500/25 bg-blue-500/10 text-blue-700",
  create: "border-emerald-500/25 bg-emerald-500/10 text-emerald-700",
  update: "border-amber-500/25 bg-amber-500/10 text-amber-700",
  delete: "border-destructive/25 bg-destructive/10 text-destructive",
};

const dreamStatusMeta: Record<MemoryDream["status"], { label: string; className: string }> = {
  pending: {
    label: "待执行",
    className: "border-border bg-muted/50 text-muted-foreground",
  },
  running: {
    label: "运行中",
    className: "border-blue-500/25 bg-blue-500/10 text-blue-700",
  },
  completed: {
    label: "已完成",
    className: "border-emerald-500/25 bg-emerald-500/10 text-emerald-700",
  },
  failed: {
    label: "执行失败",
    className: "border-destructive/25 bg-destructive/10 text-destructive",
  },
};

function formatTimestamp(value: string) {
  return new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "short",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatDate(value: string) {
  const [year = 0, month = 1, day = 1] = value.split("-").map(Number);
  return new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium" }).format(
    new Date(year, month - 1, day),
  );
}

function parseActivityTaskId(value: string): number | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  const parsed = Number(trimmed);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : undefined;
}

function StatusBadge({
  status,
  meta,
}: {
  status: string;
  meta: Record<string, { label: string; className: string }>;
}) {
  const item = meta[status];
  if (item === undefined) return null;
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium whitespace-nowrap",
        item.className,
      )}
    >
      {item.label}
    </span>
  );
}

type DialogMode = "create" | "edit" | "delete";
type OverviewTab = "dreams" | "library" | "activity";

export function MemoryOverviewPage() {
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState<OverviewTab>("dreams");

  const [dreamPage, setDreamPage] = useState(1);
  const [selectedDreamId, setSelectedDreamId] = useState<number | null>(null);
  const [deletingDream, setDeletingDream] = useState<MemoryDream | null>(null);

  const [memorySearch, setMemorySearch] = useState("");
  const [memoryKeywords, setMemoryKeywords] = useState("");
  const [memoryPage, setMemoryPage] = useState(1);

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      setMemoryKeywords(memorySearch.trim());
      setMemoryPage(1);
    }, 300);
    return () => window.clearTimeout(timeout);
  }, [memorySearch]);

  const [activityPage, setActivityPage] = useState(1);
  const [activityTaskIdInput, setActivityTaskIdInput] = useState("");
  const activityTaskId = parseActivityTaskId(activityTaskIdInput);
  const [activityOperation, setActivityOperation] = useState("all");

  const [dialogMode, setDialogMode] = useState<DialogMode | null>(null);
  const [editingItem, setEditingItem] = useState<MemoryItem | null>(null);

  const dreamsQuery = useQuery({
    queryKey: memoryKeys.dreams(dreamPage),
    queryFn: () =>
      listMemoryDreams({
        limit: DREAM_PAGE_SIZE,
        offset: (dreamPage - 1) * DREAM_PAGE_SIZE,
      }),
    placeholderData: keepPreviousData,
  });

  const overviewQuery = useQuery({
    queryKey: memoryKeys.overview(
      activityPage,
      activityTaskId ?? undefined,
      activityOperation === "all" ? undefined : activityOperation,
      memoryPage,
      memoryKeywords,
    ),
    queryFn: () =>
      getMemoryOverview({
        activityLimit: ACTIVITY_PAGE_SIZE,
        activityOffset: (activityPage - 1) * ACTIVITY_PAGE_SIZE,
        activityTaskId: activityTaskId ?? null,
        activityOperation:
          activityOperation === "all"
            ? null
            : (activityOperation as "read" | "create" | "update" | "delete"),
        itemLimit: MEMORY_PAGE_SIZE,
        itemOffset: (memoryPage - 1) * MEMORY_PAGE_SIZE,
        itemKeywords: memoryKeywords,
      }),
    placeholderData: keepPreviousData,
    gcTime: 60_000,
  });

  const detailQuery = useQuery({
    queryKey: memoryKeys.dreamDetail(selectedDreamId ?? -1),
    queryFn: () => getMemoryDream(selectedDreamId as number),
    enabled: selectedDreamId !== null,
  });

  const { isAnimating: refreshAnimating, start: startRefreshAnimation } = useRefreshAnimation();
  const refreshActive = overviewQuery.isFetching || dreamsQuery.isFetching || refreshAnimating;
  const refreshError =
    overviewQuery.isRefetchError || dreamsQuery.isRefetchError
      ? (overviewQuery.error ?? dreamsQuery.error)
      : null;
  const refresh = () =>
    startRefreshAnimation(() => Promise.all([overviewQuery.refetch(), dreamsQuery.refetch()]));

  const invalidate = () => queryClient.invalidateQueries({ queryKey: memoryKeys.all });

  const createMutation = useMutation({
    mutationFn: createMemory,
    onSuccess: async () => {
      toast.success("记忆已创建");
      await invalidate();
      setDialogMode(null);
    },
    onError: (error) => toast.error(getErrorMessage(error)),
  });

  const updateMutation = useMutation({
    mutationFn: (vars: { memoryId: number; version: number; content: string; reason: string }) =>
      updateMemory(vars.memoryId, {
        content: vars.content,
        reason: vars.reason,
        expected_version: vars.version,
      }),
    onSuccess: async () => {
      toast.success("记忆已更新");
      await invalidate();
      setDialogMode(null);
    },
    onError: (error) => toast.error(getErrorMessage(error)),
  });

  const runDreamMutation = useMutation({
    mutationFn: runMemoryDream,
    onSuccess: async () => {
      toast.success("梦境任务已提交");
      await invalidate();
    },
    onError: (error) => toast.error(getErrorMessage(error)),
  });

  const deleteDreamMutation = useMutation({
    mutationFn: deleteMemoryDream,
    onSuccess: async (_, taskId) => {
      toast.success("梦境记录已删除");
      if (selectedDreamId === taskId) {
        setSelectedDreamId(null);
      }
      const currentTotal = dreamsQuery.data?.total;
      if (currentTotal !== undefined) {
        const nextLastPage = Math.max(
          1,
          Math.ceil(Math.max(0, currentTotal - 1) / DREAM_PAGE_SIZE),
        );
        setDreamPage((currentPage) => Math.min(currentPage, nextLastPage));
      }
      await invalidate();
      setDeletingDream(null);
    },
    onError: (error) => toast.error(getErrorMessage(error)),
  });

  const deleteMutation = useMutation({
    mutationFn: (vars: { memoryId: number; version: number }) =>
      deleteMemory(vars.memoryId, vars.version),
    onSuccess: async () => {
      toast.success("记忆已删除");
      const currentMatchTotal = overviewQuery.data?.item_match_total;
      if (currentMatchTotal !== undefined) {
        const nextLastPage = Math.max(
          1,
          Math.ceil(Math.max(0, currentMatchTotal - 1) / MEMORY_PAGE_SIZE),
        );
        setMemoryPage((currentPage) => Math.min(currentPage, nextLastPage));
      }
      await invalidate();
      setDialogMode(null);
    },
    onError: (error) => toast.error(getErrorMessage(error)),
  });

  const query = overviewQuery;
  if (query.isPending || dreamsQuery.isPending) {
    return <QueryLoadingState label="正在读取记忆梦境" />;
  }
  if ((query.isError && !query.data) || (dreamsQuery.isError && !dreamsQuery.data)) {
    return (
      <QueryErrorState
        error={query.error ?? dreamsQuery.error}
        title="记忆梦境加载失败"
        onRetry={() => void refresh()}
      />
    );
  }

  const { activities, activity_total, items, item_total, item_match_total, generated_at } =
    query.data;
  const latestDream = dreamsQuery.data.latest;
  const lastDreamCompletedAt = latestDream?.completed_at ?? latestDream?.started_at ?? null;
  const latestActivity = activities[0];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-4">
      {/* ===== Heading ===== */}
      <div className="mb-2 flex items-center justify-between space-y-2">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">记忆梦境</h1>
          <p className="text-muted-foreground text-sm">
            自动读取每日运行报告，整理、合并和维护记忆
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            className={cn(refreshActive && "cursor-wait")}
            disabled={refreshActive}
            aria-busy={refreshActive}
            title="刷新记忆梦境"
            onClick={refresh}
          >
            <RefreshCwIcon
              className={cn(
                "size-4 transition-transform duration-500",
                refreshActive && "animate-spin",
              )}
            />
            刷新
          </Button>
        </div>
      </div>

      {/* ===== Overview Cards ===== */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">最近梦境</CardTitle>
            <MoonStarIcon aria-hidden className="text-muted-foreground h-4 w-4" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {latestDream ? formatDate(latestDream.target_date) : "暂无记录"}
            </div>
            <p className="text-muted-foreground text-xs">整理日期</p>
            <div className="mt-4 space-y-2 border-t pt-3">
              <div className="flex items-center justify-between gap-2 text-xs">
                <span className="text-muted-foreground">执行状态</span>
                {latestDream ? (
                  <StatusBadge status={latestDream.status} meta={dreamStatusMeta} />
                ) : (
                  <span className="font-medium">--</span>
                )}
              </div>
              <div className="flex items-center justify-between gap-2 text-xs">
                <span className="text-muted-foreground">完成时间</span>
                <span className="font-medium">
                  {lastDreamCompletedAt ? formatTimestamp(lastDreamCompletedAt) : "--"}
                </span>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">记忆条目</CardTitle>
            <ArchiveIcon aria-hidden className="text-muted-foreground h-4 w-4" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold tabular-nums">
              {item_total.toLocaleString("zh-CN")}
            </div>
            <p className="text-muted-foreground text-xs">当前记忆数量</p>
            <div className="mt-4 space-y-2 border-t pt-3">
              <div className="flex items-center justify-between gap-2 text-xs">
                <span className="text-muted-foreground">最近更新</span>
                <span className="font-medium">{formatTimestamp(generated_at)}</span>
              </div>
              <div className="flex items-center justify-between gap-2 text-xs">
                <span className="text-muted-foreground">活动记录</span>
                <span className="font-medium tabular-nums">
                  {activity_total.toLocaleString("zh-CN")} 条
                </span>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">记忆活动</CardTitle>
            <ActivityIcon aria-hidden className="text-muted-foreground h-4 w-4" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold tabular-nums">
              {activity_total.toLocaleString("zh-CN")}
            </div>
            <p className="text-muted-foreground text-xs">累计活动记录</p>
            <div className="mt-4 space-y-2 border-t pt-3">
              <div className="flex items-center justify-between gap-2 text-xs">
                <span className="text-muted-foreground">最近活动</span>
                <span className="font-medium">
                  {latestActivity ? formatMonthDayTime(latestActivity.created_at) : "--"}
                </span>
              </div>
              <div className="flex items-center justify-between gap-2 text-xs">
                <span className="text-muted-foreground">任务编号</span>
                <span className="font-medium tabular-nums">
                  {latestActivity ? `#${latestActivity.task_id}` : "--"}
                </span>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">最近活动</CardTitle>
            <ClockIcon aria-hidden className="text-muted-foreground h-4 w-4" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {latestActivity ? formatMonthDayTime(latestActivity.created_at) : "暂无"}
            </div>
            <p className="text-muted-foreground text-xs">最近记忆操作</p>
            <div className="mt-4 space-y-2 border-t pt-3">
              <div className="flex items-center justify-between gap-2 text-xs">
                <span className="text-muted-foreground">活动内容</span>
                <span
                  className="max-w-[8rem] min-w-0 flex-1 truncate text-right font-medium"
                  title={latestActivity?.content}
                >
                  {latestActivity?.content || "--"}
                </span>
              </div>
              <div className="flex items-center justify-between gap-2 text-xs">
                <span className="text-muted-foreground">任务编号</span>
                <span className="font-medium tabular-nums">
                  {latestActivity ? `#${latestActivity.task_id}` : "--"}
                </span>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* ===== Tabs ===== */}
      <Tabs
        value={activeTab}
        onValueChange={(value) => setActiveTab(value as OverviewTab)}
        className="min-h-0 flex-1"
      >
        <TabsList>
          <TabsTrigger value="dreams">梦境记录</TabsTrigger>
          <TabsTrigger value="library">记忆仓库</TabsTrigger>
          <TabsTrigger value="activity">记忆活动</TabsTrigger>
        </TabsList>

        {/* ===== Dreams ===== */}
        <TabsContent value="dreams" className="min-h-0">
          <Card className={MEMORY_TAB_CARD_CLASS}>
            <CardHeader>
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <CardTitle>梦境记录</CardTitle>
                  <CardDescription>每晚整理记忆库的独立任务</CardDescription>
                </div>
                <Button
                  type="button"
                  size="sm"
                  disabled={runDreamMutation.isPending}
                  onClick={() => runDreamMutation.mutate()}
                >
                  <PlayIcon className="size-4" />
                  {runDreamMutation.isPending ? "提交中…" : "手动运行"}
                </Button>
              </div>
            </CardHeader>
            <CardContent className="-mt-4 flex min-h-0 flex-1 flex-col overflow-hidden">
              {dreamsQuery.data.items.length === 0 ? (
                <Empty className="min-h-[220px] justify-center">
                  <EmptyHeader>
                    <EmptyTitle>暂无梦境记录</EmptyTitle>
                    <EmptyDescription>每晚 00:30 自动整理前一天的运行报告与记忆库</EmptyDescription>
                  </EmptyHeader>
                </Empty>
              ) : (
                <DreamList
                  items={dreamsQuery.data.items}
                  total={dreamsQuery.data.total}
                  page={dreamPage}
                  pageSize={DREAM_PAGE_SIZE}
                  onPageChange={setDreamPage}
                  onView={(dream) => setSelectedDreamId(dream.task_id)}
                  onDelete={(dream) => setDeletingDream(dream)}
                  deletingTaskId={deleteDreamMutation.isPending ? deletingDream?.task_id : null}
                />
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* ===== Library ===== */}
        <TabsContent value="library" className="min-h-0">
          <Card className={MEMORY_TAB_CARD_CLASS}>
            <CardHeader>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <CardTitle>记忆仓库</CardTitle>
                  <CardDescription>当前记忆库的全部记忆条目</CardDescription>
                </div>
                <div className="flex flex-wrap items-center justify-end gap-2">
                  <div className="relative">
                    <SearchIcon className="text-muted-foreground pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2" />
                    <Input
                      type="search"
                      placeholder="搜索记忆内容或形成原因"
                      value={memorySearch}
                      onChange={(e) => {
                        setMemorySearch(e.target.value);
                        setMemoryPage(1);
                      }}
                      className="w-64 pl-8"
                      maxLength={200}
                      aria-label="搜索记忆"
                    />
                  </div>
                  <Dialog
                    open={dialogMode === "create"}
                    onOpenChange={(open) => {
                      if (!open) setDialogMode(null);
                    }}
                  >
                    <DialogTrigger asChild>
                      <Button
                        type="button"
                        size="sm"
                        onClick={() => {
                          setEditingItem(null);
                          setDialogMode("create");
                        }}
                      >
                        <PlusIcon />
                        添加记忆
                      </Button>
                    </DialogTrigger>
                    {dialogMode === "create" ? (
                      <MemoryFormDialog
                        mode="create"
                        pending={createMutation.isPending}
                        onSubmit={(content, reason) => createMutation.mutate({ content, reason })}
                      />
                    ) : null}
                  </Dialog>
                </div>
              </div>
            </CardHeader>
            <CardContent className="-mt-4 flex min-h-0 flex-1 flex-col overflow-hidden">
              {item_match_total === 0 ? (
                <Empty className="min-h-[220px] justify-center">
                  <EmptyHeader>
                    <EmptyTitle>{memoryKeywords ? "没有匹配的记忆" : "记忆仓库为空"}</EmptyTitle>
                    <EmptyDescription>
                      {memoryKeywords
                        ? "尝试更换搜索关键词"
                        : "运行产生值得复用的经验后，将在此处展示"}
                    </EmptyDescription>
                  </EmptyHeader>
                </Empty>
              ) : (
                <MemoryList
                  items={items}
                  total={item_match_total}
                  page={memoryPage}
                  pageSize={MEMORY_PAGE_SIZE}
                  onPageChange={setMemoryPage}
                  onEdit={(item) => {
                    setEditingItem(item);
                    setDialogMode("edit");
                  }}
                  onDelete={(item) => {
                    setEditingItem(item);
                    setDialogMode("delete");
                  }}
                />
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* ===== Activity ===== */}
        <TabsContent value="activity" className="min-h-0">
          <Card className={MEMORY_TAB_CARD_CLASS}>
            <CardHeader>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <CardTitle>记忆活动</CardTitle>
                  <CardDescription>所有记忆读取、写入、修改与删除记录</CardDescription>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <Select
                    value={activityOperation}
                    onValueChange={(value) => {
                      setActivityOperation(value);
                      setActivityPage(1);
                    }}
                  >
                    <SelectTrigger className="w-32" aria-label="操作类型筛选">
                      <SelectValue placeholder="全部操作" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">全部操作</SelectItem>
                      <SelectItem value="read">记忆读取</SelectItem>
                      <SelectItem value="create">记忆写入</SelectItem>
                      <SelectItem value="update">记忆修改</SelectItem>
                      <SelectItem value="delete">记忆删除</SelectItem>
                    </SelectContent>
                  </Select>
                  <div className="flex items-center gap-1">
                    <Input
                      type="number"
                      min={1}
                      placeholder="任务编号"
                      aria-label="任务编号筛选"
                      value={activityTaskIdInput}
                      onChange={(e) => {
                        setActivityTaskIdInput(e.target.value);
                        setActivityPage(1);
                      }}
                      className="w-28"
                    />
                    {activityTaskIdInput !== "" ? (
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        title="清除任务筛选"
                        aria-label="清除任务筛选"
                        onClick={() => {
                          setActivityTaskIdInput("");
                          setActivityPage(1);
                        }}
                      >
                        <XIcon className="size-4" />
                      </Button>
                    ) : null}
                  </div>
                </div>
              </div>
            </CardHeader>
            <CardContent className="-mt-4 flex min-h-0 flex-1 flex-col overflow-hidden">
              {activity_total === 0 ? (
                <Empty className="min-h-[200px] justify-center">
                  <EmptyHeader>
                    <EmptyTitle>暂无记忆活动</EmptyTitle>
                    <EmptyDescription>
                      {activityTaskId !== undefined
                        ? `任务 #${activityTaskId} 没有产生记忆活动`
                        : "暂无记忆读取、写入、修改或删除记录"}
                    </EmptyDescription>
                  </EmptyHeader>
                </Empty>
              ) : (
                <ActivityTable
                  activities={activities}
                  total={activity_total}
                  page={activityPage}
                  pageSize={ACTIVITY_PAGE_SIZE}
                  onPageChange={setActivityPage}
                />
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* ===== Dream Detail Dialog ===== */}
      <Dialog
        open={selectedDreamId !== null}
        onOpenChange={(open) => {
          if (!open) setSelectedDreamId(null);
        }}
      >
        {selectedDreamId !== null ? (
          <DialogContent className="max-h-[calc(100svh-2rem)] overflow-x-hidden overflow-y-auto sm:max-w-3xl">
            <DreamDetailDialogContent
              dream={detailQuery.data?.dream ?? null}
              activities={detailQuery.data?.activities ?? []}
              activityTotal={detailQuery.data?.activity_total ?? 0}
              isPending={detailQuery.isPending}
              isError={detailQuery.isError}
              error={detailQuery.error}
              onRetry={() => void detailQuery.refetch()}
              onViewActivities={(taskId) => {
                setActivityTaskIdInput(String(taskId));
                setActivityPage(1);
                setActiveTab("activity");
                setSelectedDreamId(null);
              }}
            />
          </DialogContent>
        ) : null}
      </Dialog>

      {/* ===== Dream Delete Confirmation ===== */}
      <AlertDialog
        open={deletingDream !== null}
        onOpenChange={(open) => {
          if (!open && !deleteDreamMutation.isPending) setDeletingDream(null);
        }}
      >
        <AlertDialogContent size="sm">
          <AlertDialogHeader>
            <AlertDialogTitle>删除梦境记录</AlertDialogTitle>
            <AlertDialogDescription>
              确定删除梦境记录 #{deletingDream?.task_id}{" "}
              吗？删除后无法恢复，关联的记忆活动日志会保留。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleteDreamMutation.isPending}>取消</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              disabled={deleteDreamMutation.isPending}
              onClick={(event) => {
                event.preventDefault();
                if (deletingDream !== null) {
                  deleteDreamMutation.mutate(deletingDream.task_id);
                }
              }}
            >
              <Trash2Icon className="size-4" />
              {deleteDreamMutation.isPending ? "删除中…" : "确认删除"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* ===== Edit Dialog ===== */}
      <Dialog
        open={dialogMode === "edit" && editingItem !== null}
        onOpenChange={(open) => {
          if (!open) {
            setDialogMode(null);
            setEditingItem(null);
          }
        }}
      >
        {editingItem !== null && dialogMode === "edit" ? (
          <MemoryFormDialog
            mode="edit"
            initialContent={editingItem.content}
            initialReason={editingItem.reason}
            pending={updateMutation.isPending}
            onSubmit={(content, reason) =>
              updateMutation.mutate({
                memoryId: editingItem.id,
                version: editingItem.version,
                content,
                reason,
              })
            }
          />
        ) : null}
      </Dialog>

      {/* ===== Delete Dialog ===== */}
      <Dialog
        open={dialogMode === "delete" && editingItem !== null}
        onOpenChange={(open) => {
          if (!open) {
            setDialogMode(null);
            setEditingItem(null);
          }
        }}
      >
        {editingItem !== null && dialogMode === "delete" ? (
          <DeleteMemoryDialog
            item={editingItem}
            pending={deleteMutation.isPending}
            onSubmit={() =>
              deleteMutation.mutate({
                memoryId: editingItem.id,
                version: editingItem.version,
              })
            }
          />
        ) : null}
      </Dialog>

      {refreshError ? (
        <div
          role="alert"
          className="rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-800"
        >
          正在显示上次成功加载的数据；后台刷新失败：{getErrorMessage(refreshError)}
        </div>
      ) : null}
    </div>
  );
}

function DreamList({
  items,
  total,
  page,
  pageSize,
  onPageChange,
  onView,
  onDelete,
  deletingTaskId,
}: {
  items: MemoryDream[];
  total: number;
  page: number;
  pageSize: number;
  onPageChange: (page: number) => void;
  onView: (dream: MemoryDream) => void;
  onDelete: (dream: MemoryDream) => void;
  deletingTaskId: number | null | undefined;
}) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const currentPage = Math.min(page, totalPages);
  const pageItems = items;

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="min-h-0 min-w-0 flex-1 overflow-auto pr-1">
        <ul className="space-y-2">
          {pageItems.map((dream) => {
            const summary = dream.result ?? dream.failure_reason ?? "尚未执行整理";
            const isFailed = dream.status === "failed";
            return (
              <li key={dream.task_id} className={NESTED_LIST_ITEM_CLASS}>
                <div className="bg-primary/10 text-primary flex size-8 shrink-0 items-center justify-center rounded-md">
                  <MoonStarIcon className="size-4" />
                </div>
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <p className="text-sm font-medium">
                      {formatDate(dream.target_date)}
                      <span className="text-muted-foreground ml-2 text-xs tabular-nums">
                        任务 #{dream.task_id}
                      </span>
                    </p>
                    <StatusBadge status={dream.status} meta={dreamStatusMeta} />
                  </div>
                  <p
                    className={cn(
                      "mt-1 truncate text-xs",
                      isFailed ? "text-destructive" : "text-muted-foreground",
                    )}
                    title={summary}
                  >
                    {summary}
                  </p>
                  <div className="text-muted-foreground mt-1 flex flex-wrap items-center gap-x-3 gap-y-0.5 text-xs">
                    {dream.started_at ? (
                      <span className="inline-flex items-center gap-1">
                        <ClockIcon className="size-3" />
                        {formatTimestamp(dream.started_at)}
                      </span>
                    ) : null}
                    {dream.completed_at ? (
                      <span>至 {formatTimestamp(dream.completed_at)}</span>
                    ) : null}
                  </div>
                </div>
                <div className="flex shrink-0 items-center gap-1.5">
                  <Button type="button" variant="outline" size="sm" onClick={() => onView(dream)}>
                    查看
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    className="text-destructive hover:text-destructive"
                    disabled={deletingTaskId !== null && deletingTaskId !== undefined}
                    onClick={() => onDelete(dream)}
                  >
                    <Trash2Icon className="size-4" />
                    删除
                  </Button>
                </div>
              </li>
            );
          })}
        </ul>
      </div>
      {totalPages > 1 ? (
        <div className="border-border/60 flex items-center justify-between border-t px-1 py-2">
          <p className="text-muted-foreground text-xs tabular-nums">
            第 {currentPage.toLocaleString("zh-CN")} / {totalPages.toLocaleString("zh-CN")} 页
          </p>
          <div className="flex items-center gap-1">
            <Button
              type="button"
              variant="ghost"
              size="icon"
              title="上一页"
              aria-label="上一页"
              disabled={currentPage <= 1}
              onClick={() => onPageChange(Math.max(1, currentPage - 1))}
            >
              <ChevronLeftIcon className="size-4" />
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              title="下一页"
              aria-label="下一页"
              disabled={currentPage >= totalPages}
              onClick={() => onPageChange(Math.min(totalPages, currentPage + 1))}
            >
              <ChevronRightIcon className="size-4" />
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function DreamDetailDialogContent({
  dream,
  activities,
  activityTotal,
  isPending,
  isError,
  error,
  onRetry,
  onViewActivities,
}: {
  dream: MemoryDream | null;
  activities: MemoryActivity[];
  activityTotal: number;
  isPending: boolean;
  isError: boolean;
  error: unknown;
  onRetry: () => void;
  onViewActivities: (taskId: number) => void;
}) {
  if (isPending) {
    return (
      <DialogHeader>
        <DialogTitle>梦境记录</DialogTitle>
        <DialogDescription>正在加载详情…</DialogDescription>
      </DialogHeader>
    );
  }
  if (isError) {
    return (
      <>
        <DialogHeader>
          <DialogTitle>梦境记录加载失败</DialogTitle>
          <DialogDescription>{getErrorMessage(error)}</DialogDescription>
        </DialogHeader>
        <Button type="button" variant="outline" onClick={onRetry}>
          重新加载
        </Button>
      </>
    );
  }
  if (dream === null) return null;
  return (
    <div className="min-w-0 space-y-4">
      <DialogHeader>
        <DialogTitle className="flex items-center gap-2">
          梦境记录 #{dream.task_id}
          <StatusBadge status={dream.status} meta={dreamStatusMeta} />
        </DialogTitle>
        <DialogDescription>整理日期：{formatDate(dream.target_date)}</DialogDescription>
      </DialogHeader>

      <div className="text-muted-foreground flex flex-wrap gap-x-4 gap-y-1 text-xs">
        <span>创建于 {formatTimestamp(dream.created_at)}</span>
        {dream.started_at ? <span>开始于 {formatTimestamp(dream.started_at)}</span> : null}
        {dream.completed_at ? <span>完成于 {formatTimestamp(dream.completed_at)}</span> : null}
      </div>

      {dream.failure_reason ? (
        <div className="border-destructive/40 bg-destructive/10 text-destructive rounded-md border px-3 py-2 text-sm">
          <p className="font-medium">失败原因</p>
          <p className="mt-1 text-xs break-words">{dream.failure_reason}</p>
        </div>
      ) : null}

      <div>
        <p className="mb-1 text-sm font-medium">整理结果</p>
        <div className="bg-muted/40 rounded-md border p-3 text-sm [overflow-wrap:anywhere] break-words">
          <ReactMarkdown
            remarkPlugins={[remarkGfm]}
            components={{
              h1: ({ children }) => <h1 className="mb-3 text-lg font-semibold">{children}</h1>,
              h2: ({ children }) => <h2 className="mb-2 text-base font-semibold">{children}</h2>,
              h3: ({ children }) => <h3 className="mb-2 text-sm font-semibold">{children}</h3>,
              p: ({ children }) => <p className="mb-3 last:mb-0">{children}</p>,
              ul: ({ children }) => (
                <ul className="mb-3 list-disc space-y-1 ps-5 last:mb-0">{children}</ul>
              ),
              ol: ({ children }) => (
                <ol className="mb-3 list-decimal space-y-1 ps-5 last:mb-0">{children}</ol>
              ),
              li: ({ children }) => <li>{children}</li>,
              blockquote: ({ children }) => (
                <blockquote className="text-muted-foreground mb-3 border-s-2 ps-3 italic last:mb-0">
                  {children}
                </blockquote>
              ),
              code: ({ children }) => (
                <code className="bg-muted rounded px-1 py-0.5 font-mono text-xs">{children}</code>
              ),
              pre: ({ children }) => (
                <pre className="bg-muted mb-3 overflow-x-auto rounded-md p-3 font-mono text-xs last:mb-0">
                  {children}
                </pre>
              ),
              a: ({ children, href }) => (
                <a className="text-primary underline underline-offset-4" href={href}>
                  {children}
                </a>
              ),
            }}
          >
            {dream.result ?? "尚未生成整理结果"}
          </ReactMarkdown>
        </div>
      </div>

      <div>
        <div className="mb-1 flex items-center justify-between">
          <p className="text-sm font-medium">本次任务产生的活动</p>
          <span className="text-muted-foreground text-xs tabular-nums">
            共 {activityTotal.toLocaleString("zh-CN")} 条
          </span>
        </div>
        {activities.length === 0 ? (
          <p className="text-muted-foreground rounded-md border border-dashed px-3 py-4 text-center text-xs">
            该梦境任务没有产生记忆活动
          </p>
        ) : (
          <ul className="max-h-72 space-y-1.5 overflow-y-auto pr-1">
            {activities.map((activity) => (
              <li
                key={activity.id}
                className="hover:bg-muted/40 border-border/60 flex items-center gap-2 rounded-md border px-3 py-2"
              >
                <span
                  className={cn(
                    "inline-flex shrink-0 items-center rounded-full border px-2 py-0.5 text-xs font-medium",
                    operationBadgeClass[activity.operation] ??
                      "border-border bg-muted/50 text-muted-foreground",
                  )}
                >
                  {operationLabels[activity.operation] ?? activity.operation}
                </span>
                <span className="min-w-0 flex-1 truncate text-xs" title={activity.content}>
                  {activity.content}
                </span>
                <span className="text-muted-foreground shrink-0 text-xs tabular-nums">
                  {formatMonthDayTime(activity.created_at)}
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>

      <Button
        type="button"
        variant="outline"
        className="w-full"
        onClick={() => onViewActivities(dream.task_id)}
      >
        查看该次活动的完整记录
      </Button>
    </div>
  );
}

function MemoryList({
  items,
  total,
  page,
  pageSize,
  onPageChange,
  onEdit,
  onDelete,
}: {
  items: MemoryItem[];
  total: number;
  page: number;
  pageSize: number;
  onPageChange: (page: number) => void;
  onEdit: (item: MemoryItem) => void;
  onDelete: (item: MemoryItem) => void;
}) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const currentPage = Math.min(page, totalPages);
  const start = (currentPage - 1) * pageSize;

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="min-h-0 min-w-0 flex-1 overflow-auto pr-1">
        <ul className="space-y-2">
          {items.map((item, index) => (
            <li key={item.id} className={NESTED_LIST_ITEM_CLASS}>
              <div className="bg-primary/10 text-primary flex size-8 shrink-0 items-center justify-center rounded-md text-xs font-semibold tabular-nums">
                {start + index + 1}
              </div>
              <div className="min-w-0 flex-1">
                <p className="text-foreground/90 truncate text-sm leading-6">{item.content}</p>
                {item.reason ? (
                  <p className="text-muted-foreground mt-1 truncate text-xs leading-5">
                    <span className="font-medium">形成原因：</span>
                    {item.reason}
                  </p>
                ) : null}
                <div className="text-muted-foreground mt-1 flex flex-wrap items-center gap-x-3 gap-y-0.5 text-xs">
                  <span>
                    创建任务
                    <span className="text-foreground font-medium tabular-nums">
                      #{item.created_task_id}
                    </span>
                  </span>
                  <span>
                    更新任务
                    <span className="text-foreground font-medium tabular-nums">
                      #{item.updated_task_id}
                    </span>
                  </span>
                  <span className="inline-flex items-center gap-1 tabular-nums">
                    <ClockIcon className="size-3" />
                    {formatTimestamp(item.updated_at)}
                  </span>
                  <span className="text-muted-foreground bg-muted rounded px-1.5 py-0.5 text-xs font-medium tabular-nums">
                    v{item.version}
                  </span>
                </div>
              </div>
              <div className="flex shrink-0 items-center gap-1.5">
                <Button type="button" variant="outline" size="sm" onClick={() => onEdit(item)}>
                  编辑
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="text-destructive hover:text-destructive"
                  onClick={() => onDelete(item)}
                >
                  删除
                </Button>
              </div>
            </li>
          ))}
        </ul>
      </div>
      {totalPages > 1 ? (
        <div className="border-border/60 flex items-center justify-between border-t px-1 py-2">
          <p className="text-muted-foreground text-xs tabular-nums">
            第 {currentPage.toLocaleString("zh-CN")} / {totalPages.toLocaleString("zh-CN")} 页
          </p>
          <div className="flex items-center gap-1">
            <Button
              type="button"
              variant="ghost"
              size="icon"
              title="上一页"
              aria-label="上一页"
              disabled={currentPage <= 1}
              onClick={() => onPageChange(Math.max(1, currentPage - 1))}
            >
              <ChevronLeftIcon className="size-4" />
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              title="下一页"
              aria-label="下一页"
              disabled={currentPage >= totalPages}
              onClick={() => onPageChange(Math.min(totalPages, currentPage + 1))}
            >
              <ChevronRightIcon className="size-4" />
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function MemoryFormDialog({
  mode,
  initialContent = "",
  initialReason = "",
  pending,
  onSubmit,
}: {
  mode: "create" | "edit";
  initialContent?: string;
  initialReason?: string;
  pending: boolean;
  onSubmit: (content: string, reason: string) => void;
}) {
  const [content, setContent] = useState(initialContent);
  const [reason, setReason] = useState(initialReason);

  return (
    <DialogContent>
      <DialogHeader>
        <DialogTitle>{mode === "create" ? "添加记忆" : "编辑记忆"}</DialogTitle>
        <DialogDescription>
          {mode === "create" ? "创建一条值得后续运行复用的投资经验教训" : "修改记忆内容和形成原因"}
        </DialogDescription>
      </DialogHeader>
      <div className="space-y-4 py-2">
        <div className="space-y-2">
          <Label htmlFor="memory-content">记忆内容</Label>
          <Textarea
            id="memory-content"
            value={content}
            onChange={(e) => setContent(e.target.value)}
            placeholder="简洁的投资经验教训"
            rows={3}
          />
        </div>
        <div className="space-y-2">
          <Label htmlFor="memory-reason">形成原因</Label>
          <Textarea
            id="memory-reason"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="为什么形成这条经验"
            rows={2}
          />
        </div>
      </div>
      <DialogFooter>
        <DialogClose asChild>
          <Button type="button" variant="outline">
            取消
          </Button>
        </DialogClose>
        <Button
          type="button"
          disabled={pending || !content.trim() || !reason.trim()}
          onClick={() => onSubmit(content.trim(), reason.trim())}
        >
          {pending ? "保存中…" : mode === "create" ? "创建" : "保存"}
        </Button>
      </DialogFooter>
    </DialogContent>
  );
}

function DeleteMemoryDialog({
  item,
  pending,
  onSubmit,
}: {
  item: MemoryItem;
  pending: boolean;
  onSubmit: () => void;
}) {
  return (
    <DialogContent>
      <DialogHeader>
        <DialogTitle>删除记忆 #{item.id}</DialogTitle>
        <DialogDescription>删除后该记忆将不再出现在记忆仓库中，此操作不可撤销</DialogDescription>
      </DialogHeader>
      <div className="py-2">
        <div className="bg-muted/40 rounded-md border p-3 text-sm">
          <p className="font-medium">{item.content}</p>
          {item.reason ? (
            <p className="text-muted-foreground mt-1">
              <span className="font-medium">原因：</span>
              {item.reason}
            </p>
          ) : null}
        </div>
      </div>
      <DialogFooter>
        <DialogClose asChild>
          <Button type="button" variant="outline">
            取消
          </Button>
        </DialogClose>
        <Button type="button" variant="destructive" disabled={pending} onClick={onSubmit}>
          {pending ? "删除中…" : "确认删除"}
        </Button>
      </DialogFooter>
    </DialogContent>
  );
}

function ActivityTable({
  activities,
  total,
  page,
  pageSize,
  onPageChange,
}: {
  activities: MemoryActivity[];
  total: number;
  page: number;
  pageSize: number;
  onPageChange: (page: number) => void;
}) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const currentPage = Math.min(page, totalPages);
  const pageItems = activities;

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="min-h-0 min-w-0 flex-1 overflow-auto">
        <table className="w-full caption-bottom text-sm">
          <thead className="bg-card sticky top-0 z-10 [&_tr]:border-b">
            <tr>
              <th className="text-foreground h-8 w-32 px-2 text-center align-middle font-medium whitespace-nowrap">
                时间
              </th>
              <th className="text-foreground h-8 w-24 px-2 text-center align-middle font-medium whitespace-nowrap">
                操作
              </th>
              <th className="text-foreground h-8 w-20 px-2 text-center align-middle font-medium whitespace-nowrap">
                任务
              </th>
              <th className="text-foreground h-8 px-2 text-center align-middle font-medium whitespace-nowrap">
                内容
              </th>
              <th className="text-foreground h-8 w-32 px-2 text-center align-middle font-medium whitespace-nowrap">
                结果
              </th>
            </tr>
          </thead>
          <tbody className="[&_tr:last-child]:border-0">
            {pageItems.map((activity) => (
              <tr key={activity.id} className="hover:bg-muted/50 border-b transition-colors">
                <td className="text-muted-foreground w-32 px-2 py-3.5 text-center align-middle tabular-nums">
                  {formatMonthDayTime(activity.created_at)}
                </td>
                <td className="w-24 px-2 py-3.5 text-center align-middle">
                  <span
                    className={cn(
                      "inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium",
                      operationBadgeClass[activity.operation] ??
                        "border-border bg-muted/50 text-muted-foreground",
                    )}
                  >
                    {operationLabels[activity.operation] ?? activity.operation}
                  </span>
                </td>
                <td className="text-muted-foreground w-20 px-2 py-3.5 text-center align-middle tabular-nums">
                  #{activity.task_id}
                </td>
                <td className="max-w-0 px-2 py-3.5 text-center align-middle">
                  <p className="text-muted-foreground truncate" title={activity.content}>
                    {activity.content}
                  </p>
                </td>
                <td className="w-32 px-2 py-3.5 text-center align-middle">
                  {activity.operation === "read" && activity.result_count !== null ? (
                    <span className="text-muted-foreground text-xs tabular-nums">
                      命中 {activity.result_count} 条
                    </span>
                  ) : (
                    <span className="text-muted-foreground text-xs">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalPages > 1 ? (
        <div className="border-border/60 flex items-center justify-between border-t px-1 py-2">
          <p className="text-muted-foreground text-xs tabular-nums">
            第 {currentPage.toLocaleString("zh-CN")} / {totalPages.toLocaleString("zh-CN")} 页
          </p>
          <div className="flex items-center gap-1">
            <Button
              type="button"
              variant="ghost"
              size="icon"
              title="上一页"
              aria-label="上一页"
              disabled={currentPage <= 1}
              onClick={() => onPageChange(Math.max(1, currentPage - 1))}
            >
              <ChevronLeftIcon className="size-4" />
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              title="下一页"
              aria-label="下一页"
              disabled={currentPage >= totalPages}
              onClick={() => onPageChange(Math.min(totalPages, currentPage + 1))}
            >
              <ChevronRightIcon className="size-4" />
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
