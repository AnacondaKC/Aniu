import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MemoryOverviewPage } from "./memory-overview-page";

const api = vi.hoisted(() => ({
  getMemoryOverview: vi.fn(),
  createMemory: vi.fn(),
  updateMemory: vi.fn(),
  deleteMemory: vi.fn(),
  deleteMemoryDream: vi.fn(),
  listMemoryDreams: vi.fn(),
  runMemoryDream: vi.fn(),
  getMemoryDream: vi.fn(),
}));

vi.mock("@/lib/api", () => api);
vi.mock("sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn() },
}));

const overview = {
  generated_at: "2026-08-18T08:00:00Z",
  activity_total: 2,
  item_total: 1,
  item_match_total: 1,
  activities: [
    {
      id: 1,
      operation: "create" as const,
      memory_id: 12,
      content: "弱市缩量反弹时不要追高。",
      result_count: null,
      task_id: 101,
      created_at: "2026-08-18T07:30:00Z",
    },
    {
      id: 2,
      operation: "read" as const,
      memory_id: null,
      content: "弱市 追高",
      result_count: 1,
      task_id: 102,
      created_at: "2026-08-17T07:20:00Z",
    },
  ],
  items: [
    {
      id: 12,
      content: "弱市缩量反弹时不要追高。",
      reason: "本次运行观察到追高后回撤。",
      created_task_id: 101,
      updated_task_id: 101,
      version: 1,
      created_at: "2026-08-18T07:30:00Z",
      updated_at: "2026-08-18T07:30:00Z",
      deleted_at: null,
    },
  ],
};

const dream = {
  task_id: 20260818401,
  target_date: "2026-08-18",
  status: "completed" as const,
  result: "已整理 3 条记忆",
  failure_reason: null,
  created_at: "2026-08-17T16:30:00Z",
  started_at: "2026-08-17T16:30:00Z",
  completed_at: "2026-08-17T16:42:00Z",
};

const dreams = { items: [dream], total: 1, latest: dream };

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryOverviewPage />
    </QueryClientProvider>,
  );
}

afterEach(() => vi.clearAllMocks());

describe("MemoryOverviewPage", () => {
  it("shows dream records, the memory repository, and memory activity in tabs", async () => {
    const user = userEvent.setup();
    api.getMemoryOverview.mockResolvedValue(overview);
    api.listMemoryDreams.mockResolvedValue(dreams);

    renderPage();

    expect(await screen.findByRole("heading", { name: "记忆梦境" })).toBeInTheDocument();
    expect(api.getMemoryOverview).toHaveBeenCalledWith({
      activityLimit: 20,
      activityOffset: 0,
      activityTaskId: null,
      activityOperation: null,
      itemLimit: 10,
      itemOffset: 0,
      itemKeywords: "",
    });

    // 默认展示梦境记录
    expect(await screen.findByText("已整理 3 条记忆")).toBeInTheDocument();
    expect(screen.getAllByText("已完成").length).toBeGreaterThan(0);

    // 记忆仓库
    await user.click(screen.getByRole("tab", { name: "记忆仓库" }));
    expect(await screen.findByText("本次运行观察到追高后回撤。")).toBeInTheDocument();

    // 记忆活动
    await user.click(screen.getByRole("tab", { name: "记忆活动" }));
    expect(await screen.findByText("记忆写入")).toBeInTheDocument();
    expect(screen.getByText("记忆读取")).toBeInTheDocument();
  });

  it("renders add memory button", async () => {
    api.getMemoryOverview.mockResolvedValue(overview);
    api.listMemoryDreams.mockResolvedValue(dreams);

    renderPage();

    expect(await screen.findByRole("heading", { name: "记忆梦境" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("tab", { name: "记忆仓库" }));
    expect(screen.getByRole("button", { name: /添加记忆/ })).toBeInTheDocument();
  });

  it("submits a manual dream run from the dream card", async () => {
    const user = userEvent.setup();
    api.getMemoryOverview.mockResolvedValue(overview);
    api.listMemoryDreams.mockResolvedValue(dreams);
    api.runMemoryDream.mockResolvedValue({
      ...dream,
      status: "pending",
      result: null,
      started_at: null,
      completed_at: null,
    });

    renderPage();

    await user.click(await screen.findByRole("button", { name: "手动运行" }));

    await waitFor(() => expect(api.runMemoryDream).toHaveBeenCalledTimes(1));
  });

  it("requires confirmation before deleting a dream record", async () => {
    const user = userEvent.setup();
    api.getMemoryOverview.mockResolvedValue(overview);
    api.listMemoryDreams.mockResolvedValue(dreams);
    api.deleteMemoryDream.mockResolvedValue(undefined);

    renderPage();

    expect(await screen.findByText("已整理 3 条记忆")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "删除" }));

    expect(await screen.findByRole("alertdialog")).toHaveTextContent("删除梦境记录");
    expect(api.deleteMemoryDream).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "取消" }));
    expect(api.deleteMemoryDream).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "删除" }));
    await user.click(screen.getByRole("button", { name: "确认删除" }));

    await waitFor(() => expect(api.deleteMemoryDream).toHaveBeenCalled());
    expect(api.deleteMemoryDream.mock.calls[0]?.[0]).toBe(dream.task_id);
  });

  it("renders edit and delete buttons for each memory item", async () => {
    const user = userEvent.setup();
    api.getMemoryOverview.mockResolvedValue(overview);
    api.listMemoryDreams.mockResolvedValue(dreams);

    renderPage();

    expect(await screen.findByRole("heading", { name: "记忆梦境" })).toBeInTheDocument();
    await user.click(screen.getByRole("tab", { name: "记忆仓库" }));

    await waitFor(() => expect(screen.getByRole("button", { name: "编辑" })).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "删除" })).toBeInTheDocument();
  });

  it("resets the create form after it is closed", async () => {
    const user = userEvent.setup();
    api.getMemoryOverview.mockResolvedValue(overview);
    api.listMemoryDreams.mockResolvedValue(dreams);

    renderPage();

    await user.click(await screen.findByRole("tab", { name: "记忆仓库" }));
    await user.click(await screen.findByRole("button", { name: /添加记忆/ }));
    await user.type(screen.getByLabelText("记忆内容"), "临时记忆");
    await user.type(screen.getByLabelText("形成原因"), "临时原因");
    await user.click(screen.getByRole("button", { name: "取消" }));

    await user.click(screen.getByRole("button", { name: /添加记忆/ }));
    expect(screen.getByLabelText("记忆内容")).toHaveValue("");
    expect(screen.getByLabelText("形成原因")).toHaveValue("");
  });

  it("uses the API total when paginating dream records", async () => {
    const user = userEvent.setup();
    const olderDream = {
      ...dream,
      task_id: 20260817401,
      target_date: "2026-08-17",
      result: "更早的整理结果",
    };
    api.getMemoryOverview.mockResolvedValue(overview);
    api.listMemoryDreams.mockImplementation(({ offset }: { limit?: number; offset?: number }) =>
      Promise.resolve(
        offset === 0
          ? { items: [dream], total: 11, latest: dream }
          : { items: [olderDream], total: 11, latest: dream },
      ),
    );

    renderPage();

    expect(await screen.findByText("已整理 3 条记忆")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "下一页" }));

    await waitFor(() =>
      expect(api.listMemoryDreams).toHaveBeenLastCalledWith({ limit: 10, offset: 10 }),
    );
    expect(await screen.findByText("更早的整理结果")).toBeInTheDocument();
    expect(api.listMemoryDreams).toHaveBeenLastCalledWith({ limit: 10, offset: 10 });
  });

  it("keeps the memory search mounted and focused during a background query", async () => {
    const user = userEvent.setup();
    let resolveSearch: (value: typeof overview) => void = () => {};
    api.getMemoryOverview.mockImplementation(({ itemKeywords }: { itemKeywords: string }) =>
      itemKeywords
        ? new Promise<typeof overview>((resolve) => {
            resolveSearch = resolve;
          })
        : Promise.resolve(overview),
    );
    api.listMemoryDreams.mockResolvedValue(dreams);

    renderPage();
    await user.click(await screen.findByRole("tab", { name: "记忆仓库" }));
    const search = screen.getByRole("searchbox", { name: "搜索记忆" });
    await user.type(search, "收益");

    await waitFor(() =>
      expect(api.getMemoryOverview).toHaveBeenLastCalledWith({
        activityLimit: 20,
        activityOffset: 0,
        activityTaskId: null,
        activityOperation: null,
        itemLimit: 10,
        itemOffset: 0,
        itemKeywords: "收益",
      }),
    );
    expect(search).toHaveFocus();
    expect(screen.getByText("本次运行观察到追高后回撤。")).toBeInTheDocument();

    resolveSearch(overview);
  });

  it("shows a full-page error only when the initial overview query has no data", async () => {
    const error = new Error("overview unavailable");
    api.getMemoryOverview.mockRejectedValue(error);
    api.listMemoryDreams.mockResolvedValue(dreams);

    renderPage();

    expect(await screen.findByRole("alert")).toHaveTextContent("overview unavailable");
    expect(screen.getByRole("button", { name: "重新加载" })).toBeInTheDocument();
  });

  it("keeps cached content and shows a warning when background refresh fails", async () => {
    api.getMemoryOverview.mockResolvedValue(overview);
    api.listMemoryDreams.mockResolvedValue(dreams);
    renderPage();

    expect(await screen.findByText("已整理 3 条记忆")).toBeInTheDocument();
    api.getMemoryOverview.mockRejectedValue(new Error("refresh unavailable"));
    api.listMemoryDreams.mockRejectedValue(new Error("refresh unavailable"));

    await userEvent.click(screen.getByRole("button", { name: "刷新" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("后台刷新失败：refresh unavailable");
    expect(screen.getByText("已整理 3 条记忆")).toBeInTheDocument();
  });

  it("shows detail errors and retries the dream detail request", async () => {
    const user = userEvent.setup();
    api.getMemoryOverview.mockResolvedValue(overview);
    api.listMemoryDreams.mockResolvedValue(dreams);
    api.getMemoryDream
      .mockRejectedValueOnce(new Error("detail unavailable"))
      .mockResolvedValueOnce({
        dream,
        activities: overview.activities,
        activity_total: overview.activity_total,
      });

    renderPage();
    expect(await screen.findByText("已整理 3 条记忆")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "查看" }));

    expect(await screen.findByRole("heading", { name: "梦境记录加载失败" })).toBeInTheDocument();
    expect(screen.getByText("detail unavailable")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "重新加载" }));

    expect(
      await screen.findByRole("heading", { name: /梦境记录 #20260818401/ }),
    ).toBeInTheDocument();
    expect(api.getMemoryDream).toHaveBeenCalledTimes(2);
  });

  it("opens dream detail and jumps to filtered activity records", async () => {
    const user = userEvent.setup();
    api.getMemoryOverview.mockResolvedValue(overview);
    api.listMemoryDreams.mockResolvedValue(dreams);
    api.getMemoryDream.mockResolvedValue({
      dream: {
        ...dream,
        result: "## 整理摘要\n\n- 已整理 3 条记忆\n- 合并了重复经验",
      },
      activities: overview.activities,
      activity_total: overview.activity_total,
    });

    renderPage();

    expect(await screen.findByText("已整理 3 条记忆")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "查看" }));

    await waitFor(() => expect(api.getMemoryDream).toHaveBeenCalledWith(dream.task_id));
    expect(await screen.findByRole("dialog")).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "整理摘要" })).toBeInTheDocument();
    expect(screen.getByText("合并了重复经验")).toBeInTheDocument();
    expect(screen.getByText("本次任务产生的活动")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "查看该次活动的完整记录" }));
    expect(await screen.findByRole("tab", { name: "记忆活动" })).toHaveAttribute(
      "data-state",
      "active",
    );
    expect(screen.getByLabelText("任务编号筛选")).toHaveValue(dream.task_id);
  });
});
