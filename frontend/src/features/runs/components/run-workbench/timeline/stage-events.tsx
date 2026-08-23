import { useMemo, useState } from "react";
import {
  BookOpenCheckIcon,
  BrainIcon,
  CheckCircle2Icon,
  CircleAlertIcon,
  Loader2Icon,
  Minimize2Icon,
  WrenchIcon,
} from "lucide-react";

import {
  buildProcessRailModel,
  isRunningStatus,
  type ProcessRailModel,
  type ProcessTimelineEvent,
  type ProcessToolCall,
} from "@/features/runs/components/run-workbench/process-model";
import { cn } from "@/lib/utils";
import type { TraceStage } from "@/lib/api-types";

/** Compact body used for one-line tool and metadata rows. */
const processRailContentClass =
  "h-[18px] max-w-[70%] min-w-0 truncate text-[10px] font-normal leading-[18px] text-muted-foreground";
const processRailLabelClass =
  "inline-flex h-[18px] shrink-0 items-center gap-1 text-[10px] font-medium leading-[18px] text-foreground";
const processRailExpandedContentClass =
  "min-w-0 max-w-[80ch] flex-1 self-start whitespace-pre-wrap break-words border-s border-border/30 ps-2.5 text-[10px] font-normal leading-[17px] text-muted-foreground";
const MARKET_SESSION_GATE_STEP_ID = "market_session_closed";
function ThinkingLabel({ className }: { className?: string }) {
  return (
    <span className={cn(processRailLabelClass, className)}>
      <BrainIcon className="size-2.5 shrink-0" aria-hidden />
      深度思考
    </span>
  );
}

function ThinkingBadge({ className }: { className?: string }) {
  return (
    <span
      className={cn(
        "inline-flex h-4 shrink-0 items-center rounded-sm border border-zinc-500/25 bg-zinc-500/[0.08] px-1.5 text-[9px] leading-none font-medium text-zinc-700",
        className,
      )}
    >
      深度思考
    </span>
  );
}

function ToolCallLabel({ className }: { className?: string }) {
  return (
    <span className={cn(processRailLabelClass, className)}>
      <WrenchIcon className="size-2.5 shrink-0" aria-hidden />
      工具调用
    </span>
  );
}

function SystemToolLabel({ className }: { className?: string }) {
  return (
    <span className={cn(processRailLabelClass, className)}>
      <WrenchIcon className="size-2.5 shrink-0" aria-hidden />
      系统工具
    </span>
  );
}

function SystemToolBadge({ label }: { label: string }) {
  return (
    <span className="inline-flex h-4 shrink-0 items-center rounded-sm border border-yellow-500/25 bg-yellow-500/[0.08] px-1.5 text-[9px] leading-none font-medium text-yellow-700">
      {label}
    </span>
  );
}

function ContextCompactionLabel({ className }: { className?: string }) {
  return (
    <span className={cn(processRailLabelClass, className)}>
      <Minimize2Icon className="size-2.5 shrink-0" aria-hidden />
      原文压缩
    </span>
  );
}

const TOOL_SOURCE_LABELS: Record<ProcessToolCall["source"], string> = {
  aggregate: "聚合研判",
  mx: "妙想接口",
  public: "公开数据",
  internal: "系统",
};

const toolMetadataValueClass = "max-w-[11rem] min-w-0 shrink truncate font-medium text-foreground";
const toolRequestParameterClass = "max-w-[15rem] min-w-0 shrink truncate text-muted-foreground";
const toolMetricClass = "shrink-0 whitespace-nowrap tabular-nums text-muted-foreground";

function ToolSourceBadge({ source }: { source: ProcessToolCall["source"] }) {
  const sourceLabel = TOOL_SOURCE_LABELS[source];
  const sourceClass =
    source === "aggregate"
      ? "border-emerald-500/25 bg-emerald-500/[0.08] text-emerald-700 "
      : source === "mx"
        ? "border-violet-500/25 bg-violet-500/[0.08] text-violet-700 "
        : source === "public"
          ? "border-orange-500/25 bg-orange-500/[0.08] text-orange-700 "
          : "border-slate-500/25 bg-slate-500/[0.08] text-slate-700 ";

  return (
    <span
      className={cn(
        "inline-flex h-4 shrink-0 items-center rounded-sm border px-1.5 text-[9px] leading-none font-medium",
        sourceClass,
      )}
    >
      {sourceLabel}
    </span>
  );
}

function isMarketSessionBlocked(call: ProcessToolCall) {
  return call.status === "blocked" && (call.toolName === "trade" || call.toolName === "cancel");
}

function agentToolResult(call: ProcessToolCall) {
  if (call.status === "completed") return "完成";
  if (isMarketSessionBlocked(call)) return "非交易时间";
  if (call.status === "blocked") return "已阻止";
  if (call.status === "failed") return "失败";
  return "进行中";
}

function formatToolReturnDuration(call: ProcessToolCall) {
  if (!call.startedAt || !call.endedAt) return "--";

  const startedAt = Date.parse(call.startedAt);
  const endedAt = Date.parse(call.endedAt);
  if (!Number.isFinite(startedAt) || !Number.isFinite(endedAt) || endedAt < startedAt) {
    return "--";
  }

  const durationMs = endedAt - startedAt;
  if (durationMs < 1_000) return `${durationMs} ms`;

  const seconds = durationMs / 1_000;
  const precision = seconds >= 10 ? 1 : 2;
  return `${Number(seconds.toFixed(precision))} 秒`;
}

function formatModelContentCharacters(call: ProcessToolCall) {
  if (call.modelContentCharacters === null) return "--";
  return `${(call.modelContentCharacters / 1_000).toFixed(2)}K字符`;
}

function AgentToolRow({ call }: { call: ProcessToolCall }) {
  const result = agentToolResult(call);
  const isFailure = call.status === "failed";
  const isWarning = call.status === "blocked";
  const isRunning = isRunningStatus(call.status);
  const modelContentCharacters = formatModelContentCharacters(call);
  const returnDuration = formatToolReturnDuration(call);
  const ResultIcon = isRunning
    ? Loader2Icon
    : isFailure || isWarning
      ? CircleAlertIcon
      : CheckCircle2Icon;

  return (
    <div
      className="min-w-0 py-0.5 text-[10px] leading-[17px]"
      data-testid={`agent-tool-${call.id}`}
      role="group"
      aria-label={`Agent 工具：${call.displayName}（${call.toolName}）`}
    >
      <div className="flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-0.5">
        <ToolCallLabel />
        <ToolSourceBadge source={call.source} />
        <span className={toolMetadataValueClass} title={`工具：${call.displayName}`}>
          {call.displayName}
        </span>
        {call.queryParameters ? (
          <code className={toolRequestParameterClass} title={`请求参数：${call.queryParameters}`}>
            {call.queryParameters}
          </code>
        ) : null}
        <span className={toolMetricClass} title={`提交 Agent 字符数：${modelContentCharacters}`}>
          提交 Agent {modelContentCharacters}
        </span>
        <span className={toolMetricClass} title={`返回耗时：${returnDuration}`}>
          返回 {returnDuration}
        </span>
        <span
          className={cn(
            "inline-flex max-w-[10rem] min-w-0 shrink-0 items-center gap-1 truncate font-medium",
            isFailure
              ? "text-destructive"
              : isWarning
                ? "text-yellow-700"
                : isRunning
                  ? "text-muted-foreground"
                  : "text-emerald-700",
          )}
          title={`工具结果：${result}`}
        >
          <ResultIcon className={cn("size-3 shrink-0", isRunning && "animate-spin")} aria-hidden />
          <span className="truncate">{result}</span>
        </span>
      </div>
    </div>
  );
}

function MarketSessionGateRow({ summary }: { summary: string | null }) {
  const content = summary || "非交易时段，交易和撤单写操作已被阻止；仍可继续分析。";

  return (
    <div className="flex min-w-0 flex-nowrap items-center gap-x-1.5 overflow-hidden py-0.5 text-[10px] leading-[17px]">
      <SystemToolLabel />
      <SystemToolBadge label="交易门禁" />
      <span className={processRailContentClass} title={content}>
        {content}
      </span>
    </div>
  );
}

function MemoryToolRow({ call }: { call: ProcessToolCall }) {
  const result = agentToolResult(call);
  const isFailure = call.status === "failed";
  const isWarning = call.status === "blocked";
  const isRunning = isRunningStatus(call.status);
  const ResultIcon = isRunning
    ? Loader2Icon
    : isFailure || isWarning
      ? CircleAlertIcon
      : CheckCircle2Icon;
  const operationValue = call.queryParameters?.match(/operation=([^ ·]+)/)?.[1];
  const operation =
    call.toolName === "memory_read"
      ? "记忆读取"
      : operationValue === "delete"
        ? "记忆删除"
        : operationValue === "update"
          ? "记忆修改"
          : "记忆写入";
  const target =
    call.queryParameters ?? (call.toolName === "memory_read" ? "读取的记忆" : "要写入的记忆");
  return (
    <div
      className="min-w-0 py-0.5 text-[10px] leading-[17px]"
      role="group"
      aria-label={`记忆工具：${operation}`}
    >
      <div className="flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-0.5">
        <span className={processRailLabelClass}>
          <BookOpenCheckIcon className="size-2.5 shrink-0" aria-hidden />
          系统工具
        </span>
        <SystemToolBadge label="记忆系统" />
        <span className={processRailLabelClass}>{operation}</span>
        <code className={toolRequestParameterClass} title={target}>
          {target}
        </code>
        <span
          className={cn(
            "inline-flex shrink-0 items-center gap-1 font-medium",
            isFailure
              ? "text-destructive"
              : isWarning
                ? "text-yellow-700"
                : isRunning
                  ? "text-muted-foreground"
                  : "text-emerald-700",
          )}
        >
          <ResultIcon className={cn("size-3", isRunning && "animate-spin")} aria-hidden />
          {result}
        </span>
      </div>
    </div>
  );
}

function InternalToolRow({ call }: { call: ProcessToolCall }) {
  return (
    <div className="flex items-center gap-1.5 py-0.5 text-[11px] leading-[18px]">
      <ToolCallLabel />
      <span className="text-foreground truncate font-medium" title={call.displayName}>
        {call.displayName}
      </span>
      <span className="text-muted-foreground truncate" title={call.toolName}>
        {call.toolName}
      </span>
      <ToolSourceBadge source={call.source} />
      {call.queryParameters ? (
        <span className={processRailContentClass} title={call.queryParameters}>
          {call.queryParameters}
        </span>
      ) : null}
    </div>
  );
}

function ToolRow({ call }: { call: ProcessToolCall }) {
  if (call.toolName === "memory_read" || call.toolName === "memory_write") {
    return <MemoryToolRow call={call} />;
  }
  if (call.source === "internal") {
    return <InternalToolRow call={call} />;
  }
  return <AgentToolRow call={call} />;
}

const PROCESS_RAIL_DEFAULT_VISIBLE = 5;

function formatThinkingBody(text: string) {
  // Provider output often inserts blank-only lines between every sentence. They
  // add visual height without adding structure in this compact process rail.
  return text
    .replace(/\r\n?/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n[ \t]*\n+/g, "\n")
    .trim();
}

function ThinkingRow({ text, running = false }: { text: string; running?: boolean }) {
  const [open, setOpen] = useState(false);
  const body = formatThinkingBody(text);
  if (!body && !running) return null;

  return (
    <button
      type="button"
      className={cn(
        "flex w-full items-center gap-1.5 py-0.5 text-start text-[10px] leading-[17px] transition-colors",
        open && "items-start",
        "cursor-pointer hover:opacity-90",
      )}
      onClick={() => setOpen((value) => !value)}
      aria-expanded={open}
    >
      <ThinkingLabel className={cn("shrink-0", open && "self-start")} />
      <ThinkingBadge className={cn(open && "self-start")} />
      <span
        className={cn(toolMetadataValueClass, "shrink-0", open && "self-start")}
        title="思考内容"
      >
        思考内容
      </span>
      {open ? (
        <span className={cn(processRailExpandedContentClass, "max-h-[32rem] overflow-auto")}>
          {body || "…"}
          {running ? (
            <span className="animate-caret-blink ms-0.5 inline-block text-sky-600">▌</span>
          ) : null}
        </span>
      ) : (
        <span className={processRailContentClass} title={body || "…"}>
          {body || "…"}
        </span>
      )}
    </button>
  );
}

function ExtraRow({ event }: { event: Extract<ProcessTimelineEvent, { kind: "extra" }> }) {
  const isMarketSessionGate = event.step.step_id === MARKET_SESSION_GATE_STEP_ID;
  if (isMarketSessionGate) {
    return <MarketSessionGateRow summary={event.step.summary} />;
  }

  const isContextCompaction = event.step.step_id.startsWith("context_compaction:");
  const detail = [event.step.title || event.step.type, event.step.summary]
    .filter(Boolean)
    .join(" · ");

  return (
    <div className="text-muted-foreground flex items-center gap-1.5 py-0.5 text-[11px] leading-[18px]">
      {isContextCompaction ? (
        <ContextCompactionLabel />
      ) : (
        <span className="text-muted-foreground inline-flex h-[18px] shrink-0 items-center text-[9px] leading-[18px] font-normal">
          其他
        </span>
      )}
      <span className={processRailContentClass} title={detail}>
        {detail}
      </span>
    </div>
  );
}

function TimelineEventRow({ event, live }: { event: ProcessTimelineEvent; live: boolean }) {
  if (event.kind === "thinking") {
    return <ThinkingRow text={event.text} running={live && isRunningStatus(event.status)} />;
  }
  if (event.kind === "tool") {
    return <ToolRow call={event.call} />;
  }
  return <ExtraRow event={event} />;
}

/** Flat chronological process list; default show first N, expand for the rest. */
function ProcessRail({ model }: { model: ProcessRailModel }) {
  const [showAll, setShowAll] = useState(false);
  const items = model.timeline;
  const expanded = showAll || model.isLive;
  const visible = expanded ? items : items.slice(0, PROCESS_RAIL_DEFAULT_VISIBLE);
  const hiddenCount = Math.max(0, items.length - visible.length);
  const hasTimeline = items.length > 0;

  if (!model.hasProcess && !hasTimeline) {
    return model.isLive ? (
      <div className="text-muted-foreground flex w-full items-center gap-1 py-0.5 text-[10px] xl:w-3/4">
        <Loader2Icon className="size-2.5 animate-spin" />
        准备中…
      </div>
    ) : null;
  }

  return (
    <div className="mb-0 w-full space-y-0 xl:w-3/4">
      {visible.map((event) => (
        <TimelineEventRow key={event.id} event={event} live={model.isLive} />
      ))}
      {!model.isLive && hiddenCount > 0 ? (
        <button
          type="button"
          className="text-muted-foreground hover:text-muted-foreground inline-flex py-0.5 text-[9px] font-normal transition-colors"
          onClick={() => setShowAll(true)}
        >
          展开其余 {hiddenCount} 项
        </button>
      ) : null}
      {!model.isLive && showAll && items.length > PROCESS_RAIL_DEFAULT_VISIBLE ? (
        <button
          type="button"
          className="text-muted-foreground hover:text-muted-foreground inline-flex py-0.5 text-[9px] font-normal transition-colors"
          onClick={() => setShowAll(false)}
        >
          收起
        </button>
      ) : null}
    </div>
  );
}

/**
 * Renders the live process rail (LLM inputs / thinking / tool calls) for one
 * stage, plus "正在生成结论…" / "进行中…" trailers while the stage is live.
 */
export function StageEvents({
  stage,
  liveStepDeltaByStepId = {},
}: {
  stage: TraceStage;
  liveStepDeltaByStepId?: Record<string, string>;
}) {
  const model = useMemo(
    () => buildProcessRailModel(stage, liveStepDeltaByStepId),
    [stage, liveStepDeltaByStepId],
  );

  const hasProcessOrInputs = model.hasProcess;

  return (
    <div className="border-muted-foreground/20 ms-1 border-s py-0 ps-2">
      <ProcessRail model={model} />

      {model.isLive && model.visibleResults.length === 0 && !model.processActive ? (
        <div className="text-muted-foreground flex w-full items-center gap-1 py-0.5 text-[10px] xl:w-3/4">
          <Loader2Icon className="size-2.5 animate-spin" />
          正在生成结论…
        </div>
      ) : null}

      {model.isLive && model.processActive ? (
        <div className="text-muted-foreground flex w-full items-center gap-1 py-0.5 text-[10px] xl:w-3/4">
          <Loader2Icon className="size-2.5 animate-spin" />
          进行中…
        </div>
      ) : null}

      {!hasProcessOrInputs && !model.isLive ? (
        <div className="text-muted-foreground w-full py-0.5 text-[10px] xl:w-3/4">暂无过程记录</div>
      ) : null}
    </div>
  );
}
