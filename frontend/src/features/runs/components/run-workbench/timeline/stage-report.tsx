import { useState } from "react";

import {
  isRunningStatus,
  stepLiveText,
} from "@/features/runs/components/run-workbench/process-model";
import { StreamingContent } from "@/features/runs/components/run-workbench/streaming";
import type { TraceStage, TraceStep } from "@/lib/api-types";

/** Markdown report for one stage, collapsed until the user opens it. */
export function StageReport({
  stage,
  steps,
  liveStepDeltaByStepId = {},
  isLive = stage.status === "running",
  displayMode = "collapsible",
  showLabel = true,
}: {
  stage: TraceStage;
  steps: TraceStep[];
  liveStepDeltaByStepId?: Record<string, string>;
  isLive?: boolean;
  displayMode?: "collapsible" | "full";
  showLabel?: boolean;
}) {
  const [expanded, setExpanded] = useState(false);
  const showFullReport = displayMode === "full" || expanded;

  if (steps.length === 0) return null;

  const blocks = steps.map((step) => {
    const text = stepLiveText(stage, step, liveStepDeltaByStepId);
    const streaming = isLive && isRunningStatus(step.status);
    return { step, text, streaming };
  });
  const anyContent = blocks.some((block) => block.text.trim() || block.streaming);
  if (!anyContent) return null;

  return (
    <div className="mt-0 space-y-1.5">
      {blocks.map(({ step, text, streaming }) => {
        const body = text || (streaming ? "" : "当前还没有最终结论。");
        return (
          <ReportBlock
            key={step.step_id}
            body={body}
            streaming={streaming}
            expanded={showFullReport}
            collapsible={displayMode === "collapsible"}
            showLabel={showLabel}
            onToggleExpanded={() => setExpanded((value) => !value)}
          />
        );
      })}
    </div>
  );
}

function ReportBlock({
  body,
  streaming,
  expanded,
  collapsible,
  showLabel,
  onToggleExpanded,
}: {
  body: string;
  streaming: boolean;
  expanded: boolean;
  collapsible: boolean;
  showLabel: boolean;
  onToggleExpanded: () => void;
}) {
  if (collapsible && !expanded) {
    return (
      <button
        type="button"
        onClick={onToggleExpanded}
        className="text-muted-foreground hover:text-muted-foreground inline-flex items-center gap-1 py-0 text-[9px] leading-[18px] font-normal transition-colors"
      >
        阶段报告
        <span className="text-muted-foreground">展开 ↓</span>
      </button>
    );
  }

  return (
    <div>
      {showLabel ? <ReportLabel onCollapse={collapsible ? onToggleExpanded : undefined} /> : null}
      <StreamingContent
        content={body}
        streaming={streaming}
        scrollable={false}
        variant={collapsible ? "process" : "report"}
      />
    </div>
  );
}

function ReportLabel({ onCollapse }: { onCollapse?: (() => void) | undefined }) {
  const content = (
    <>
      阶段报告
      {onCollapse ? <span className="text-muted-foreground">收起 ↑</span> : null}
    </>
  );

  if (onCollapse) {
    return (
      <button
        type="button"
        onClick={onCollapse}
        className="text-muted-foreground hover:text-muted-foreground mb-0.5 inline-flex items-center gap-1 text-[9px] leading-[18px] font-normal transition-colors"
      >
        {content}
      </button>
    );
  }

  return (
    <p className="text-muted-foreground mb-0.5 inline-flex items-center gap-1 text-[9px] leading-[18px] font-normal">
      {content}
    </p>
  );
}
