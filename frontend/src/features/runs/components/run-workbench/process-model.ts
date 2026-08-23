import type { TraceStage, TraceStep, TraceStockApiCall, TraceToolSource } from "@/lib/api-types";

export type ProcessToolCall = {
  id: string;
  intentLine: string;
  status: string;
  source: TraceToolSource;
  toolName: string;
  displayName: string;
  queryParameters: string | null;
  modelContentCharacters: number | null;
  startedAt: string | null;
  endedAt: string | null;
  stockApiCalls: TraceStockApiCall[];
};

/** A display-safe trace entry, kept in its recorded order for the live rail. */
export type ProcessTimelineEvent =
  | {
      kind: "thinking";
      id: string;
      text: string;
      status: string;
      step: TraceStep;
      order: number;
    }
  | {
      kind: "tool";
      id: string;
      call: ProcessToolCall;
      order: number;
    }
  | {
      kind: "extra";
      id: string;
      step: TraceStep;
      order: number;
    };

export type ProcessRailModel = {
  timeline: ProcessTimelineEvent[];
  isLive: boolean;
  hasProcess: boolean;
  /** True while deep-thinking or tool calls are still in flight. */
  processActive: boolean;
  /** Result steps that are safe to render as the stage report. */
  visibleResults: TraceStep[];
};

function liveKey(stageId: string, stepId: string) {
  return `${stageId}::${stepId}`;
}

export function stepLiveText(
  stage: TraceStage,
  step: TraceStep,
  liveStepDeltaByStepId: Record<string, string>,
) {
  const key = liveKey(stage.stage_id, step.step_id);
  const live = liveStepDeltaByStepId[key] ?? liveStepDeltaByStepId[step.step_id] ?? "";
  return `${step.content ?? ""}${live}`;
}

export function isRunningStatus(status: string | null | undefined) {
  return status === "running" || status === "requested";
}

function toolCallFromStep(step: TraceStep): ProcessToolCall {
  const toolCall = step.tool_call;
  return {
    id: toolCall?.call_id ?? step.step_id,
    intentLine: toolCall?.intent_line ?? step.title ?? "工具调用",
    status: step.status,
    source: toolCall?.source ?? "internal",
    toolName: toolCall?.tool_name ?? step.title ?? "工具调用",
    displayName: toolCall?.display_name ?? step.title ?? "工具调用",
    queryParameters: toolCall?.query_parameters ?? null,
    modelContentCharacters: toolCall?.model_content_characters ?? null,
    startedAt: step.started_at,
    endedAt: step.ended_at,
    stockApiCalls: toolCall?.stock_api_calls ?? [],
  };
}
function isTerminalStatus(step: TraceStep) {
  return step.type === "status" && step.title === "最终状态";
}

export function buildProcessRailModel(
  stage: TraceStage,
  liveStepDeltaByStepId: Record<string, string>,
): ProcessRailModel {
  const steps = stage.steps ?? [];
  const thinkingSteps = steps.filter((step) => step.type === "thinking");
  const results = steps.filter((step) => step.type === "result");
  const thinkingRunning = thinkingSteps.some((step) => isRunningStatus(step.status));

  const timeline = steps.flatMap((step, order): ProcessTimelineEvent[] => {
    if (step.type === "thinking") {
      return [
        {
          kind: "thinking",
          id: step.step_id,
          text: stepLiveText(stage, step, liveStepDeltaByStepId),
          status: step.status,
          step,
          order,
        },
      ];
    }
    if (step.type === "tool") {
      const call = toolCallFromStep(step);
      return [{ kind: "tool", id: call.id, call, order }];
    }
    // The run summary and stage marker already expose terminal state.
    if (step.type === "status" && !isTerminalStatus(step)) {
      return [{ kind: "extra", id: step.step_id, step, order }];
    }
    return [];
  });

  const stageRunning = stage.status === "running";
  const reportDone = results.length > 0 && results.every((step) => !isRunningStatus(step.status));
  const isLive = stageRunning && !reportDone;
  const toolRunning = timeline.some(
    (event) => event.kind === "tool" && isRunningStatus(event.call.status),
  );
  const processActive = thinkingRunning || toolRunning;
  const visibleResults = processActive
    ? []
    : results.filter((step) => {
        const text = stepLiveText(stage, step, liveStepDeltaByStepId).trim();
        return Boolean(text) || isRunningStatus(step.status);
      });

  return {
    timeline,
    isLive,
    hasProcess: timeline.length > 0,
    processActive,
    visibleResults,
  };
}
