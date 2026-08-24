import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { RunDetail, TraceStage } from "@/lib/api-types";

import { StageTimeline } from "./stage-timeline";

const runStage: TraceStage = {
  stage_id: "run:na",
  key: "run",
  status: "completed",
  started_at: "2026-07-25T10:00:00Z",
  ended_at: "2026-07-25T10:00:20Z",
  steps: [],
};

const summaryStage: TraceStage = {
  stage_id: "summary:na",
  key: "summary",
  status: "completed",
  started_at: "2026-07-25T10:00:20Z",
  ended_at: "2026-07-25T10:00:30Z",
  steps: [],
};

function makeRun(overrides: Partial<RunDetail> = {}): RunDetail {
  return {
    run_id: 20260725101,
    task_id: 20260725101,
    trigger_source: "manual",
    schedule_id: null,
    status: "COMPLETED",
    current_state: "Completed",
    summary: "# Markdown 报告",
    summary_render_mode: "markdown",
    started_at: "2026-07-25T10:00:00Z",
    completed_at: "2026-07-25T10:00:30Z",
    tool_calls_count: 3,
    thinking_count: 2,
    total_tokens: 1200,
    trade_count: 1,
    failure_reason: null,
    trace: {
      schema_version: 3,
      event_seq: 4,
      current_stage_id: null,
      stages: [runStage, summaryStage],
    },
    ...overrides,
  };
}

function renderTimeline(run: RunDetail) {
  return render(
    <StageTimeline run={run} now={new Date("2026-07-25T10:00:30Z")} liveStepDeltaByStepId={{}} />,
  );
}

describe("StageTimeline", () => {
  it("renders the two-stage summary and raw HTML report", () => {
    renderTimeline(
      makeRun({
        summary:
          '<section class="report-grid" style="display:flex;color:red">' +
          "<h2>执行总结</h2><p>成交 1 笔</p></section>",
        summary_render_mode: "html",
      }),
    );

    expect(screen.getByText("执行完成")).toBeInTheDocument();
    expect(screen.getByText("工具3次")).toBeInTheDocument();
    const heading = screen.getByRole("heading", { name: "执行总结" });
    expect(heading).toBeInTheDocument();
    expect(heading.closest("section")).toHaveClass("report-grid");
    expect(heading.closest("section")).toHaveStyle({
      display: "flex",
      color: "rgb(255, 0, 0)",
    });
    expect(screen.getByText("成交 1 笔")).toBeInTheDocument();
  });

  it("renders Markdown when Summary is degraded", () => {
    const degradedSummary: TraceStage = {
      ...summaryStage,
      status: "degraded",
      steps: [
        {
          step_id: "markdown_fallback",
          type: "status",
          title: "回退 Markdown",
          status: "completed",
          summary: "HTML 总结生成失败，已回退 Markdown",
          content: null,
          tool_call: null,
          started_at: summaryStage.started_at,
          ended_at: summaryStage.ended_at,
        },
      ],
    };
    renderTimeline(
      makeRun({
        summary: "## Markdown 回退报告",
        trace: {
          schema_version: 3,
          event_seq: 5,
          current_stage_id: null,
          stages: [runStage, degradedSummary],
        },
      }),
    );

    expect(screen.getByRole("heading", { name: "Markdown 回退报告" })).toBeInTheDocument();
  });

  it("uses warning styling for market-session blocks and distinct source badges", () => {
    const blockedRunStage: TraceStage = {
      ...runStage,
      status: "failed",
      steps: [
        {
          step_id: "thinking:1",
          type: "thinking",
          title: "深度思考",
          status: "completed",
          summary: null,
          content: "正在判断交易条件。",
          tool_call: null,
          started_at: runStage.started_at,
          ended_at: runStage.ended_at,
        },
        {
          step_id: "tool:trade",
          type: "tool",
          title: "模拟交易",
          status: "blocked",
          summary: null,
          content: null,
          tool_call: {
            call_id: "trade-1",
            intent_line: "模拟交易 · 买入 600519",
            source: "mx",
            tool_name: "trade",
            display_name: "模拟交易",
            query_parameters: "instruction=买入 600519 100",
          },
          started_at: runStage.started_at,
          ended_at: runStage.ended_at,
        },
        {
          step_id: "tool:quote",
          type: "tool",
          title: "实时行情",
          status: "completed",
          summary: null,
          content: null,
          tool_call: {
            call_id: "quote-1",
            intent_line: "实时行情 · 600519.SH",
            source: "public",
            tool_name: "stock_quote",
            display_name: "实时行情",
            query_parameters: "symbols=600519.SH",
          },
          started_at: runStage.started_at,
          ended_at: runStage.ended_at,
        },
      ],
    };

    renderTimeline(
      makeRun({
        status: "FAILED",
        current_state: "Failed",
        summary: null,
        failure_reason: "测试失败",
        trace: {
          schema_version: 3,
          event_seq: 7,
          current_stage_id: null,
          stages: [blockedRunStage, summaryStage],
        },
      }),
    );

    expect(screen.getByText("非交易时间").parentElement).toHaveClass("text-yellow-700");
    expect(screen.getByText("公开数据")).toHaveClass("border-orange-500/25");
    expect(
      screen
        .getAllByText("深度思考")
        .some((element) => element.classList.contains("border-zinc-500/25")),
    ).toBe(true);
  });

  it("shows the recorded failure reason when Run fails", () => {
    renderTimeline(
      makeRun({
        status: "FAILED",
        current_state: "Failed",
        summary: null,
        failure_reason: "运行模型不可用",
        trace: {
          schema_version: 3,
          event_seq: 2,
          current_stage_id: null,
          stages: [{ ...runStage, status: "failed" }],
        },
      }),
    );

    const alerts = screen.getAllByRole("alert", { name: "失败原因" });
    expect(alerts).toHaveLength(2);
    for (const alert of alerts) expect(alert).toHaveTextContent("运行模型不可用");
  });

  it("freezes stale running steps as soon as manual stop is requested", () => {
    const staleRunningStage: TraceStage = {
      ...runStage,
      status: "running",
      ended_at: null,
      steps: [
        {
          step_id: "thinking:stop",
          type: "thinking",
          title: "深度思考",
          status: "running",
          summary: null,
          content: "正在分析停止边界。",
          tool_call: null,
          started_at: runStage.started_at,
          ended_at: null,
        },
        {
          step_id: "tool:stop",
          type: "tool",
          title: "行情查询",
          status: "running",
          summary: null,
          content: null,
          tool_call: {
            call_id: "stop-call",
            intent_line: "行情查询 · 600519.SH",
            source: "public",
            tool_name: "stock_quote",
            display_name: "行情查询",
            query_parameters: "symbols=600519.SH",
          },
          started_at: runStage.started_at,
          ended_at: null,
        },
      ],
    };
    const run = makeRun({
      status: "RUNNING",
      current_state: "Run",
      completed_at: null,
      summary: null,
      trace: {
        schema_version: 3,
        event_seq: 3,
        current_stage_id: staleRunningStage.stage_id,
        stages: [staleRunningStage],
      },
    });

    const { container } = render(
      <StageTimeline
        run={run}
        now={new Date("2026-07-25T10:00:30Z")}
        liveStepDeltaByStepId={{}}
        isStopping
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: "展开阶段" }));

    expect(screen.getAllByText("停止中").length).toBeGreaterThan(0);
    expect(screen.getByText("已停止")).toBeInTheDocument();
    expect(container.querySelector(".animate-spin")).toBeNull();
    expect(container.querySelector(".animate-caret-blink")).toBeNull();
  });

  it("uses the terminal Run status when an aborted snapshot contains stale running stages", () => {
    const staleStage: TraceStage = { ...runStage, status: "running", ended_at: null };
    renderTimeline(
      makeRun({
        status: "ABORTED",
        current_state: "Failed",
        completed_at: "2026-07-25T10:00:25Z",
        summary: null,
        trace: {
          schema_version: 3,
          event_seq: 3,
          current_stage_id: staleStage.stage_id,
          stages: [staleStage],
        },
      }),
    );

    expect(screen.getByText("已中止")).toBeInTheDocument();
    expect(screen.queryByText("执行中")).toBeNull();
  });
});
