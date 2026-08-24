import { describe, expect, it } from "vitest";

import type { TraceStage, TraceStep } from "@/lib/api-types";
import { buildProcessRailModel } from "./process-model";

function step(overrides: Partial<TraceStep>): TraceStep {
  return {
    step_id: "tool-step",
    type: "tool",
    title: "工具调用",
    status: "completed",
    summary: null,
    content: null,
    tool_call: null,
    started_at: null,
    ended_at: null,
    ...overrides,
  };
}

function stage(steps: TraceStep[]): TraceStage {
  return {
    stage_id: "run:na",
    key: "run",
    status: "completed",
    started_at: null,
    ended_at: null,
    steps,
  };
}

describe("buildProcessRailModel", () => {
  it("uses the server-projected tool intent without parsing raw payloads", () => {
    const model = buildProcessRailModel(
      stage([
        step({
          status: "failed",
          tool_call: {
            call_id: "call-42",
            intent_line: "妙想资讯查询 · semiconductor earnings",
            source: "mx",
            tool_name: "search_news",
            display_name: "资讯搜索",
            query_parameters: "query=semiconductor earnings",
          },
        }),
      ]),
      {},
    );

    expect(model.timeline).toEqual([
      {
        kind: "tool",
        id: "call-42",
        order: 0,
        call: {
          id: "call-42",
          intentLine: "妙想资讯查询 · semiconductor earnings",
          status: "failed",
          source: "mx",
          toolName: "search_news",
          displayName: "资讯搜索",
          queryParameters: "query=semiconductor earnings",
          modelContentCharacters: null,
          startedAt: null,
          endedAt: null,
          stockApiCalls: [],
        },
      },
    ]);
  });

  it("keeps thinking, status, and result lifecycle behavior", () => {
    const model = buildProcessRailModel(
      stage([
        step({
          step_id: "thinking",
          type: "thinking",
          status: "completed",
          content: "完整思考",
        }),
        step({
          step_id: "status",
          type: "status",
          title: "阶段状态",
          summary: "已处理",
        }),
        step({
          step_id: "result",
          type: "result",
          content: "阶段报告",
        }),
      ]),
      {},
    );

    expect(model.timeline.map((event) => event.kind)).toEqual(["thinking", "extra"]);
    expect(model.visibleResults.map((item) => item.step_id)).toEqual(["result"]);
    expect(model.hasProcess).toBe(true);
  });

  it("clamps active process state when the parent Run is stopping", () => {
    const runningStage: TraceStage = {
      ...stage([
        step({
          step_id: "thinking",
          type: "thinking",
          status: "running",
          content: "未完成思考",
        }),
      ]),
      status: "running",
    };

    const model = buildProcessRailModel(runningStage, {}, false);

    expect(model.isLive).toBe(false);
    expect(model.processActive).toBe(false);
  });

  it("omits the redundant terminal status from the process rail", () => {
    const model = buildProcessRailModel(
      stage([
        step({
          step_id: "final-status",
          type: "status",
          title: "最终状态",
          summary: "运行已完成。",
        }),
      ]),
      {},
    );

    expect(model.timeline).toEqual([]);
    expect(model.hasProcess).toBe(false);
  });
});
