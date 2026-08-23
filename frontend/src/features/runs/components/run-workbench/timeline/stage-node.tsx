import { useEffect, useMemo, useRef, useState } from "react";
import { ChevronRightIcon, CircleAlertIcon, LoaderCircleIcon } from "lucide-react";

import { Badge } from "@/components/ui/badge";

import {
  buildProcessRailModel,
  isRunningStatus,
  type ProcessToolCall,
} from "@/features/runs/components/run-workbench/process-model";
import { formatRunDuration, formatTimeWithSeconds } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { RunDetail, TraceStage, TraceStageKey } from "@/lib/api-types";

import { StageEvents } from "./stage-events";
import { StageReport } from "./stage-report";

type StageStatus = TraceStage["status"];

const STAGE_DISPLAY_NAME: Record<TraceStageKey, string> = {
  run: "执行阶段",
  summary: "总结阶段",
};

function toneFor(status: StageStatus, isCurrent: boolean) {
  if (status === "running" && isCurrent) {
    return {
      label: "进行中",
      status: "text-sky-600 dark:text-sky-400",
      row: "bg-sky-500/[0.04]",
    };
  }

  switch (status) {
    case "running":
      return {
        label: "进行中",
        status: "text-muted-foreground",
        row: "",
      };
    case "completed":
      return {
        label: "完成",
        status: "text-emerald-500 dark:text-emerald-400",
        row: "",
      };
    case "degraded":
      return {
        label: "已回退",
        status: "text-amber-600 dark:text-amber-400",
        row: "bg-amber-500/[0.03]",
      };
    case "failed":
      return {
        label: "失败",
        status: "text-rose-500 dark:text-rose-300",
        row: "bg-destructive/[0.03]",
      };
    case "skipped":
      return {
        label: "跳过",
        status: "text-amber-600 dark:text-amber-400",
        row: "",
      };
    default:
      return {
        label: "待处理",
        status: "text-muted-foreground",
        row: "",
      };
  }
}

const TOOL_NAMES_MAX = 5;

type ToolCallGroup = {
  name: string;
  calls: ProcessToolCall[];
  running: boolean;
};

function toolSummaryName(intentLine: string) {
  const rawName = intentLine.split(/\s+·\s+/, 1)[0] ?? "";
  return rawName.trim().replace(/^妙想/, "") || "工具调用";
}

function groupToolCalls(calls: ProcessToolCall[], isLive: boolean) {
  const grouped = new Map<string, ProcessToolCall[]>();

  for (const call of calls) {
    const name = toolSummaryName(call.intentLine);
    grouped.set(name, [...(grouped.get(name) ?? []), call]);
  }

  return Array.from(grouped, ([name, groupedCalls]): ToolCallGroup => ({
    name,
    calls: groupedCalls,
    running: isLive && groupedCalls.some((call) => isRunningStatus(call.status)),
  }));
}

function ToolCallText({ calls, isLive }: { calls: ProcessToolCall[]; isLive: boolean }) {
  if (calls.length === 0) {
    return <span className="sr-only">无工具调用</span>;
  }

  const groups = groupToolCalls(calls, isLive);
  const visible = groups.slice(0, TOOL_NAMES_MAX);
  const remainder = groups.length - visible.length;

  return (
    <span className="text-muted-foreground inline-flex min-w-0 items-center gap-2 overflow-hidden text-[9px] leading-none font-medium">
      {visible.map((group) => {
        const countLabel = group.calls.length > 1 ? ` ×${group.calls.length}` : "";
        const title = group.calls.map((call) => call.intentLine).join("\n");

        return (
          <span
            key={group.name}
            className={cn(
              "inline-flex max-w-[6rem] shrink-0 items-center gap-0.5 truncate whitespace-nowrap",
              group.running && "text-sky-600 dark:text-sky-400",
            )}
            title={title}
          >
            <span className="truncate">{group.name}</span>
            {countLabel ? <span className="shrink-0 tabular-nums">{countLabel}</span> : null}
            {group.running ? (
              <LoaderCircleIcon aria-hidden className="size-2.5 shrink-0 animate-spin" />
            ) : null}
          </span>
        );
      })}
      {remainder > 0 ? (
        <span className="text-muted-foreground inline-flex shrink-0 items-center whitespace-nowrap">
          +{remainder}
        </span>
      ) : null}
    </span>
  );
}

function StageOutcomeBadge({ stage }: { stage: TraceStage }) {
  if (stage.status !== "degraded") return null;
  return (
    <Badge
      variant="outline"
      aria-label="HTML 总结失败，已回退 Markdown"
      title={stage.steps.find((step) => step.step_id === "markdown_fallback")?.summary ?? undefined}
      className="h-4 gap-1 rounded-sm border-amber-500/25 bg-amber-500/[0.08] px-1.5 py-0 text-[9px] leading-none font-medium text-amber-700 dark:text-amber-300"
    >
      Markdown 回退
    </Badge>
  );
}

export function StageNode({
  run,
  stage,
  now,
  liveStepDeltaByStepId,
}: {
  run: RunDetail;
  stage: TraceStage;
  now: Date;
  liveStepDeltaByStepId: Record<string, string>;
}) {
  const isCurrent =
    run.status === "RUNNING" &&
    run.trace.current_stage_id === stage.stage_id &&
    stage.status === "running";

  const tone = toneFor(stage.status, isCurrent);
  const defaultExpanded = stage.status === "running" || stage.status === "failed";
  const [override, setOverride] = useState<boolean | null>(null);
  const expanded = override ?? defaultExpanded;

  const prevStatusRef = useRef(stage.status);
  useEffect(() => {
    if (prevStatusRef.current !== stage.status) {
      prevStatusRef.current = stage.status;
      setOverride(null);
    }
  }, [stage.status]);

  const aggregate = useMemo(() => {
    const model = buildProcessRailModel(stage, liveStepDeltaByStepId);
    const thinkingCount = model.timeline.filter((event) => event.kind === "thinking").length;
    const toolCalls = model.timeline.flatMap((event) =>
      event.kind === "tool" ? [event.call] : [],
    );

    return {
      thinkingCount,
      toolCalls,
      toolCount: toolCalls.length,
    };
  }, [stage, liveStepDeltaByStepId]);
  const duration =
    stage.started_at != null ? formatRunDuration(stage.started_at, stage.ended_at, now) : "--";
  const failureReason =
    run.status === "FAILED" && stage.status === "failed"
      ? run.failure_reason?.trim() || "任务执行失败，但没有记录具体失败原因。"
      : null;

  return (
    <li className="list-none">
      <button
        type="button"
        onClick={() => setOverride(!expanded)}
        aria-expanded={expanded}
        title={`耗时 ${duration} · 思考 ${aggregate.thinkingCount} 次 · 工具 ${aggregate.toolCount} 次 · ${tone.label}`}
        className={cn(
          "focus-visible:bg-surface-soft/60 relative flex w-full items-center gap-0 rounded-md px-2 py-1 text-start transition-colors outline-none focus-visible:outline-none",
          expanded && "bg-surface-soft/40",
          tone.row,
        )}
      >
        <span className="me-2 inline-flex shrink-0 items-center gap-2">
          <ChevronRightIcon
            aria-hidden
            className={cn(
              "text-muted-foreground size-3 shrink-0 transition-transform duration-150",
              expanded && "text-muted-foreground rotate-90",
            )}
          />
          <span className="text-muted-foreground text-[10px] font-medium tabular-nums">
            {formatTimeWithSeconds(stage.started_at)}
          </span>
          <span className="text-foreground truncate font-sans text-[11px] font-medium tracking-[-0.005em]">
            {STAGE_DISPLAY_NAME[stage.key]}
          </span>
          <StageOutcomeBadge stage={stage} />
        </span>

        <span className="inline-flex max-w-[24rem] min-w-0 shrink items-center overflow-hidden">
          <ToolCallText calls={aggregate.toolCalls} isLive={isCurrent} />
        </span>

        <span
          className={cn(
            "text-muted-foreground inline-flex shrink-0 items-center gap-2 text-[9px] font-medium tabular-nums",
            aggregate.toolCalls.length > 0 && "ms-2",
          )}
        >
          <span className="whitespace-nowrap">耗时 {duration}</span>
          <span className="whitespace-nowrap">思考 {aggregate.thinkingCount} 次</span>
          <span className="whitespace-nowrap">工具 {aggregate.toolCount} 次</span>
        </span>

        <span
          aria-label={tone.label}
          title={tone.label}
          className={cn(
            "ms-1 inline-flex w-5 shrink-0 items-center justify-end leading-none",
            tone.status,
          )}
        >
          {stage.status === "running" ? null : stage.status === "completed" ? (
            <CheckDot />
          ) : stage.status === "failed" ? (
            <CrossDot />
          ) : stage.status === "degraded" || stage.status === "skipped" ? (
            <SkipDot />
          ) : (
            <span className="size-1.5 rounded-full border border-current/60" />
          )}
        </span>
      </button>

      {expanded ? (
        <div className="mx-2 px-2 pt-0.5 pb-1">
          {failureReason ? (
            <div
              role="alert"
              aria-label="失败原因"
              className="border-destructive/70 bg-destructive/[0.04] mt-1 flex min-w-0 items-start gap-2 border-s-2 px-2 py-1.5"
            >
              <CircleAlertIcon aria-hidden className="text-destructive mt-0.5 size-3.5 shrink-0" />
              <div className="min-w-0">
                <p className="text-destructive text-[10px] leading-4 font-medium">失败原因</p>
                <p className="text-foreground text-[12px] leading-[18px] break-words whitespace-pre-wrap">
                  {failureReason}
                </p>
              </div>
            </div>
          ) : null}
          {stage.steps.length > 0 ? (
            <>
              <StageEvents stage={stage} liveStepDeltaByStepId={liveStepDeltaByStepId} />
              {stage.key !== "summary" ? (
                <StageReport
                  stage={stage}
                  steps={stage.steps.filter((step) => step.type === "result")}
                  liveStepDeltaByStepId={liveStepDeltaByStepId}
                />
              ) : null}
            </>
          ) : (
            <div className="text-muted-foreground py-2 text-[12px]">
              {stage.status === "running" ? "阶段进行中…" : "暂无过程记录"}
            </div>
          )}
        </div>
      ) : null}
    </li>
  );
}

function CheckDot() {
  return (
    <svg viewBox="0 0 16 16" className="size-3" fill="none" aria-hidden>
      <circle cx="8" cy="8" r="7" fill="currentColor" opacity={0.16} />
      <path
        d="M5 8.2 7 10.2 11 6"
        stroke="currentColor"
        strokeWidth={1.6}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function CrossDot() {
  return (
    <svg viewBox="0 0 16 16" className="size-3" fill="none" aria-hidden>
      <circle cx="8" cy="8" r="7" fill="currentColor" opacity={0.16} />
      <path
        d="M5.5 5.5 10.5 10.5M10.5 5.5 5.5 10.5"
        stroke="currentColor"
        strokeWidth={1.6}
        strokeLinecap="round"
      />
    </svg>
  );
}

function SkipDot() {
  return (
    <svg viewBox="0 0 16 16" className="size-3" fill="none" aria-hidden>
      <circle cx="8" cy="8" r="6.5" stroke="currentColor" strokeWidth={1.4} />
      <path d="M5.5 8h5" stroke="currentColor" strokeWidth={1.4} strokeLinecap="round" />
    </svg>
  );
}
