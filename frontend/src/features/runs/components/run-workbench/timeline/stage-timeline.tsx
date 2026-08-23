import { useState } from "react";
import { ChevronRightIcon, CircleAlertIcon } from "lucide-react";

import { Empty, EmptyHeader, EmptyTitle } from "@/components/ui/empty";
import { formatRunDuration } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { RunDetail } from "@/lib/api-types";

import { StreamingContent } from "../streaming";
import { StageNode } from "./stage-node";

/**
 * Compact stage process summary followed by the terminal report or failure reason.
 * The parent owns scrolling and the ticking clock.
 */

export function StageTimeline({
  run,
  now,
  liveStepDeltaByStepId,
}: {
  run: RunDetail;
  now: Date;
  liveStepDeltaByStepId: Record<string, string>;
}) {
  const trace = run.trace;
  const stages = trace.stages;
  const isFailed = run.status === "FAILED";
  const recordedFailureReason = run.failure_reason?.trim() || null;
  const shouldExpandFailedStages = isFailed && recordedFailureReason !== null;
  const [stagesExpandedOverride, setStagesExpandedOverride] = useState<boolean | null>(null);
  const stagesExpanded = stagesExpandedOverride ?? shouldExpandFailedStages;
  const stageListId = `run-stage-list-${run.run_id}`;
  const toggleStages = () => {
    setStagesExpandedOverride((override) => !(override ?? shouldExpandFailedStages));
  };
  if (stages.length === 0 && !isFailed) {
    return (
      <Empty className="min-h-[220px] justify-center">
        <EmptyHeader>
          <EmptyTitle>暂无执行阶段</EmptyTitle>
        </EmptyHeader>
      </Empty>
    );
  }

  const totalDuration = formatRunDuration(run.started_at, run.completed_at, now);
  const runStage = stages.find((stage) => stage.key === "run");
  const summaryStage = stages.find((stage) => stage.key === "summary");
  const runStatusLabel =
    runStage?.status === "completed"
      ? "执行完成"
      : runStage?.status === "failed"
        ? "执行失败"
        : "执行中";
  const finalReportSteps = summaryStage?.steps.filter((step) => step.type === "result") ?? [];
  // The run summary is the authoritative final report; the summary stage result
  // is only a fallback while a terminal snapshot is settling.
  const finalReportContent =
    run.summary?.trim() ||
    finalReportSteps
      .map((step) => step.content?.trim() || step.summary?.trim() || "")
      .filter(Boolean)
      .join("\n\n");
  const showFinalReport =
    (summaryStage?.status === "completed" || summaryStage?.status === "degraded") &&
    finalReportContent.length > 0;
  const failureReason = recordedFailureReason || "任务执行失败，但没有记录具体失败原因。";

  return (
    <>
      <div className="text-muted-foreground flex flex-wrap items-center gap-1 px-2 pb-1.5 text-[12px] leading-5">
        <button
          type="button"
          aria-label={stagesExpanded ? "收起阶段" : "展开阶段"}
          aria-controls={stageListId}
          aria-expanded={stagesExpanded}
          title={stagesExpanded ? "收起阶段" : "展开阶段"}
          onClick={toggleStages}
          className="hover:bg-muted/60 focus-visible:ring-ring/50 -ms-1 inline-flex size-5 shrink-0 items-center justify-center rounded-sm transition-colors outline-none focus-visible:ring-[3px]"
        >
          <ChevronRightIcon
            aria-hidden
            className={cn(
              "size-3.5 transition-transform duration-150",
              stagesExpanded && "rotate-90",
            )}
          />
        </button>
        <button
          type="button"
          aria-controls={stageListId}
          aria-expanded={stagesExpanded}
          title={stagesExpanded ? "收起阶段" : "展开阶段"}
          onClick={toggleStages}
          className="focus-visible:ring-ring/50 inline-flex min-w-0 flex-wrap items-center gap-1 rounded-sm text-start outline-none focus-visible:ring-[3px]"
        >
          <span className="text-foreground font-semibold">运行摘要</span>
          <span aria-hidden>·</span>
          <span className="font-medium">{runStatusLabel}</span>
          <span aria-hidden>·</span>
          <span className="font-medium tabular-nums">工具{run.tool_calls_count}次</span>
          <span aria-hidden>·</span>
          <span className="font-medium tabular-nums">交易{run.trade_count}次</span>
          <span aria-hidden>·</span>
          <span className="font-medium tabular-nums">总耗时{totalDuration}</span>
        </button>
      </div>

      {stagesExpanded ? (
        <ol id={stageListId} className="flex flex-col gap-0 py-0">
          {stages.map((stage) => (
            <StageNode
              key={stage.stage_id}
              run={run}
              stage={stage}
              now={now}
              liveStepDeltaByStepId={liveStepDeltaByStepId}
            />
          ))}
        </ol>
      ) : null}

      {isFailed ? (
        <section className="px-2 pt-4 pb-3">
          <h2 className="text-foreground mb-3 font-sans text-base font-semibold tracking-[-0.01em]">
            最终运行报告
          </h2>
          <div
            role="alert"
            aria-label="失败原因"
            className="border-destructive/70 bg-destructive/[0.04] flex items-start gap-2 border-s-2 px-3 py-2.5"
          >
            <CircleAlertIcon aria-hidden className="text-destructive mt-0.5 size-4 shrink-0" />
            <div className="min-w-0">
              <p className="text-destructive text-[12px] font-medium">失败原因</p>
              <p className="text-foreground mt-1 max-h-48 overflow-auto text-[13px] leading-5 break-words whitespace-pre-wrap">
                {failureReason}
              </p>
            </div>
          </div>
        </section>
      ) : showFinalReport ? (
        <section className="px-2 pt-4 pb-3">
          <h2 className="text-foreground mb-3 font-sans text-base font-semibold tracking-[-0.01em]">
            最终运行报告
          </h2>
          <StreamingContent
            content={finalReportContent}
            streaming={false}
            scrollable={false}
            renderMode={run.summary_render_mode ?? "markdown"}
          />
        </section>
      ) : null}
    </>
  );
}
