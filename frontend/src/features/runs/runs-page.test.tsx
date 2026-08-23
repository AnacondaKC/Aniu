import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { listRuns } from "@/lib/api";
import type { RunSummary } from "@/lib/api-types";

import { RunsPage } from "./runs-page";

vi.mock("@/lib/api", () => ({
  deleteRun: vi.fn(),
  listRuns: vi.fn(),
}));

vi.mock("@/features/runs/components/run-start-button", () => ({
  RunStartButton: () => null,
}));

vi.mock("@/features/runs/components/run-workbench", () => ({
  RunWorkbenchPanel: () => null,
}));

const baseRun: RunSummary = {
  run_id: 20260729107,
  task_id: 20260729107,
  trigger_source: "manual",
  schedule_id: null,
  status: "COMPLETED",
  current_state: "Completed",
  summary: null,
  summary_render_mode: "markdown",
  started_at: "2026-07-29T09:19:00Z",
  completed_at: "2026-07-29T09:25:32Z",
  tool_calls_count: 13,
  thinking_count: 9,
  total_tokens: 200_000,
  trade_count: 0,
};

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <RunsPage />
    </QueryClientProvider>,
  );
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.clearAllMocks();
});

describe("RunsPage", () => {
  it("shows zero trades consistently across completed runs", async () => {
    vi.mocked(listRuns).mockResolvedValue([
      baseRun,
      {
        ...baseRun,
        run_id: 20260729106,
        task_id: 20260729106,
      },
    ]);

    renderPage();

    expect((await screen.findAllByText("交易 0 次")).length).toBe(2);
  });

  it("places page actions at the horizontal list edges and preserves scroll continuity", async () => {
    const user = userEvent.setup();
    const runs = Array.from({ length: 45 }, (_, index) => ({
      ...baseRun,
      run_id: baseRun.run_id - index,
      task_id: baseRun.task_id - index,
    }));
    vi.mocked(listRuns).mockImplementation((limit = 20, offset = 0) =>
      Promise.resolve(runs.slice(offset, offset + limit)),
    );
    vi.spyOn(HTMLElement.prototype, "scrollWidth", "get").mockReturnValue(4_000);
    vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockReturnValue(800);

    renderPage();

    const firstPageList = await screen.findByTestId("runs-scroll-list");
    const firstPageButtons = within(firstPageList).getAllByRole("button");
    expect(within(firstPageList).queryByRole("button", { name: "上一页任务" })).toBeNull();
    expect(firstPageButtons.at(-1)).toHaveAccessibleName("下一页任务");
    expect(screen.queryByText(/第 \d+ 页/)).toBeNull();

    await user.click(within(firstPageList).getByRole("button", { name: "下一页任务" }));
    await screen.findByRole("button", {
      name: `查看运行 ${runs[20]?.task_id} 的执行记录`,
    });

    const secondPageList = screen.getByTestId("runs-scroll-list");
    const secondPageButtons = within(secondPageList).getAllByRole("button");
    expect(secondPageButtons[0]).toHaveAccessibleName("上一页任务");
    expect(secondPageButtons.at(-1)).toHaveAccessibleName("下一页任务");
    expect(secondPageList.scrollLeft).toBe(0);

    await user.click(within(secondPageList).getByRole("button", { name: "上一页任务" }));

    await waitFor(() => {
      const restoredList = screen.getByTestId("runs-scroll-list");
      expect(within(restoredList).queryByRole("button", { name: "上一页任务" })).toBeNull();
      expect(restoredList.scrollLeft).toBe(3_200);
    });
    expect(
      screen.getByRole("button", {
        name: `查看运行 ${runs[19]?.task_id} 的执行记录`,
      }),
    ).toHaveAttribute("aria-pressed", "true");
  });
});
