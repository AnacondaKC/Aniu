import { useMemo, useState, type ReactNode } from "react";
import {
  CheckCircle2Icon,
  CircleDashedIcon,
  Clock3Icon,
  RefreshCwIcon,
  SlidersHorizontalIcon,
  TriangleAlertIcon,
  XIcon,
} from "lucide-react";

import type {
  CreateSchedulePayload,
  StrategySchedule,
  UpdateSchedulePayload,
} from "@/lib/api-types";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Spinner } from "@/components/ui/spinner";
import { Switch } from "@/components/ui/switch";
import { SectionLabel } from "@/features/settings/components/section-label";

const MAX_CUSTOM_SCHEDULE_TIMES = 48;

function generatePreview(intervalMinutes: number) {
  const sessions = [
    { start: 9 * 60 + 30, end: 11 * 60 },
    { start: 13 * 60, end: 14 * 60 + 30 },
  ];

  const result: string[] = [];

  for (const { start, end } of sessions) {
    for (let minutes = start; minutes <= end; minutes += intervalMinutes) {
      const hour = Math.floor(minutes / 60);
      const minute = minutes % 60;
      result.push(`${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`);
    }
  }

  return result;
}

const statusBadgeClass =
  "h-5 gap-1 rounded-full border px-2 text-[11px] font-medium leading-none [&>svg]:size-3";

/** Runtime-sync lifecycle badge keeps scheduler state visible beside the task name. */
function SyncStatusBadge({ schedule }: { schedule?: StrategySchedule | undefined }) {
  if (!schedule) {
    return (
      <Badge
        key="unsaved"
        variant="outline"
        className={cn(
          statusBadgeClass,
          "border-amber-500/30 bg-amber-500/10 text-amber-700 dark:text-amber-300",
        )}
      >
        <CircleDashedIcon />
        未保存
      </Badge>
    );
  }
  if (schedule.sync_error) {
    return (
      <Badge
        key="sync-error"
        variant="outline"
        className={cn(statusBadgeClass, "border-destructive/30 bg-destructive/10 text-destructive")}
      >
        <TriangleAlertIcon />
        同步失败
      </Badge>
    );
  }
  if (schedule.revision > 0 && schedule.runtime_synced_revision === schedule.revision) {
    return (
      <Badge
        key="synced"
        variant="outline"
        className={cn(
          statusBadgeClass,
          "border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
        )}
      >
        <CheckCircle2Icon />
        已同步
      </Badge>
    );
  }
  return (
    <Badge
      key="pending"
      variant="outline"
      className={cn(
        statusBadgeClass,
        "border-sky-500/30 bg-sky-500/10 text-sky-700 dark:text-sky-300",
      )}
    >
      <RefreshCwIcon />
      待同步
    </Badge>
  );
}

export type ScheduleSubmission =
  | { scheduleId?: undefined; payload: CreateSchedulePayload }
  | { scheduleId: number; payload: UpdateSchedulePayload };

type ScheduleSettingsCardsProps = {
  schedules: StrategySchedule[];
  savePending: boolean;
  writeDisabled: boolean;
  onSubmit: (args: ScheduleSubmission) => Promise<unknown>;
};

type ModeCardShellProps = {
  title: string;
  enabled: boolean;
  busy: boolean;
  schedule?: StrategySchedule | undefined;
  onToggle: (enabled: boolean) => void;
  children: ReactNode;
};

/** Shared card chrome: header with mode switch, collapsible settings body. */
function ModeCardShell({ title, enabled, busy, schedule, onToggle, children }: ModeCardShellProps) {
  return (
    <div className="border-border/60 bg-card/50 hover:border-border overflow-hidden rounded-xl border shadow-xs">
      <div className="flex items-center gap-3 px-4 py-3.5 sm:px-5">
        <div className="flex min-w-0 flex-1 flex-wrap items-center gap-2">
          <span className="truncate text-sm font-semibold tracking-tight">{title}</span>
          {enabled && schedule !== undefined ? <SyncStatusBadge schedule={schedule} /> : null}
        </div>
        <Switch
          checked={enabled}
          disabled={busy}
          onCheckedChange={onToggle}
          aria-label={`${enabled ? "停用" : "启用"}${title}`}
          className="shrink-0"
        />
      </div>

      {schedule?.sync_error ? (
        <div className="border-destructive/20 bg-destructive/5 text-destructive border-t px-4 py-2 text-xs sm:px-5">
          调度同步失败：{schedule.sync_error}
        </div>
      ) : null}

      {enabled ? (
        <div className="border-border/60 flex flex-col gap-5 border-t px-4 pt-4 pb-4 sm:px-5">
          {children}
        </div>
      ) : null}
    </div>
  );
}

/** 定时任务：选择时/分后点击添加，按用户指定的时点每天运行。 */
function CustomTimeCard({
  schedule,
  enabled,
  busy,
  onSubmit,
  onToggle,
}: {
  schedule?: StrategySchedule | undefined;
  enabled: boolean;
  busy: boolean;
  onSubmit: ScheduleSettingsCardsProps["onSubmit"];
  onToggle: (enabled: boolean) => void;
}) {
  const [times, setTimes] = useState<string[]>(() => schedule?.custom_schedule_times ?? []);
  const [hour, setHour] = useState("9");
  const [minute, setMinute] = useState("30");
  const [pickerError, setPickerError] = useState<string | null>(null);

  const addTime = () => {
    const hourValue = Number(hour);
    const minuteValue = Number(minute);
    if (!Number.isInteger(hourValue) || hourValue < 0 || hourValue > 23) {
      setPickerError("小时需为 0–23 的整数");
      return;
    }
    if (!Number.isInteger(minuteValue) || minuteValue < 0 || minuteValue > 59) {
      setPickerError("分钟需为 0–59 的整数");
      return;
    }
    setPickerError(null);
    const value = `${String(hourValue).padStart(2, "0")}:${String(minuteValue).padStart(2, "0")}`;
    if (times.includes(value)) {
      return;
    }
    if (times.length >= MAX_CUSTOM_SCHEDULE_TIMES) {
      setPickerError(`最多添加 ${MAX_CUSTOM_SCHEDULE_TIMES} 个时点`);
      return;
    }
    setTimes([...times, value].sort());
  };

  const removeTime = (value: string) => {
    setTimes((current) => current.filter((item) => item !== value));
  };

  const handleSave = async () => {
    if (times.length === 0) {
      return;
    }
    const payload = {
      enabled: true,
      task_type: "market_analysis" as const,
      interval_minutes: schedule?.interval_minutes ?? 15,
      schedule_times: times,
    };
    try {
      if (schedule) {
        await onSubmit({
          scheduleId: schedule.schedule_id,
          payload: { ...payload, expected_revision: schedule.revision },
        });
        return;
      }
      await onSubmit({ payload });
    } catch {
      // The page-level mutation owns error and conflict feedback.
    }
  };

  return (
    <ModeCardShell
      title="定时任务"
      enabled={enabled}
      busy={busy}
      schedule={schedule}
      onToggle={onToggle}
    >
      <section className="flex flex-col gap-3" aria-label="定时任务调度规则">
        <SectionLabel icon={<SlidersHorizontalIcon className="size-3.5" />}>调度规则</SectionLabel>
        <div className="flex flex-wrap items-center gap-2">
          <Label htmlFor="custom-hour" className="sr-only">
            时
          </Label>
          <Input
            id="custom-hour"
            type="number"
            min={0}
            max={23}
            step={1}
            disabled={busy}
            value={hour}
            onChange={(event) => setHour(event.target.value)}
            className="h-8 w-20 text-center"
          />
          <span className="text-muted-foreground">:</span>
          <Label htmlFor="custom-minute" className="sr-only">
            分
          </Label>
          <Input
            id="custom-minute"
            type="number"
            min={0}
            max={59}
            step={1}
            disabled={busy}
            value={minute}
            onChange={(event) => setMinute(event.target.value)}
            className="h-8 w-20 text-center"
          />
          <Button variant="outline" size="sm" disabled={busy} onClick={addTime}>
            添加
          </Button>
        </div>
        {pickerError ? <p className="text-destructive text-xs">{pickerError}</p> : null}
        <p className="text-muted-foreground text-xs">
          输入时和分后点击“添加”，按指定时点每天（工作日）运行
        </p>
      </section>

      <section className="flex flex-col gap-3" aria-label="已选时点">
        <SectionLabel icon={<Clock3Icon className="size-3.5" />}>
          已选时点
          <span className="text-muted-foreground/80 font-normal tabular-nums">
            {times.length} 个
          </span>
        </SectionLabel>
        {times.length > 0 ? (
          <div className="flex flex-wrap gap-2">
            {times.map((time) => (
              <span
                key={time}
                className="border-border/60 bg-muted/40 text-foreground/80 inline-flex h-7 items-center gap-1 rounded-lg border px-2 text-xs font-medium tabular-nums"
              >
                {time}
                <button
                  type="button"
                  onClick={() => removeTime(time)}
                  aria-label={`移除 ${time}`}
                  className="text-muted-foreground hover:text-foreground rounded p-0.5 transition-colors"
                >
                  <XIcon className="size-3" />
                </button>
              </span>
            ))}
          </div>
        ) : (
          <p className="text-muted-foreground text-xs">请添加至少一个时点</p>
        )}
      </section>

      <div className="border-border/60 flex justify-end border-t pt-4">
        <Button disabled={busy || times.length === 0} onClick={() => void handleSave()}>
          {busy ? <Spinner className="size-4" /> : null}
          保存定时任务设置
        </Button>
      </div>
    </ModeCardShell>
  );
}

/** 间隔任务：按间隔自动生成时点。 */
function IntervalCard({
  schedule,
  enabled,
  busy,
  onSubmit,
  onToggle,
}: {
  schedule?: StrategySchedule | undefined;
  enabled: boolean;
  busy: boolean;
  onSubmit: ScheduleSettingsCardsProps["onSubmit"];
  onToggle: (enabled: boolean) => void;
}) {
  const [intervalText, setIntervalText] = useState(() =>
    schedule?.interval_minutes ? String(schedule.interval_minutes) : "15",
  );
  const interval = Math.max(15, Number(intervalText) || 15);
  const preview = useMemo(() => generatePreview(interval), [interval]);

  const handleSave = async () => {
    const payload = {
      enabled: true,
      task_type: "market_analysis" as const,
      interval_minutes: interval,
      schedule_times: null,
    };
    try {
      if (schedule) {
        await onSubmit({
          scheduleId: schedule.schedule_id,
          payload: { ...payload, expected_revision: schedule.revision },
        });
        return;
      }
      await onSubmit({ payload });
    } catch {
      // The page-level mutation owns error and conflict feedback.
    }
  };

  return (
    <ModeCardShell
      title="间隔任务"
      enabled={enabled}
      busy={busy}
      schedule={schedule}
      onToggle={onToggle}
    >
      <section className="flex flex-col gap-3" aria-label="间隔任务调度规则">
        <SectionLabel icon={<SlidersHorizontalIcon className="size-3.5" />}>调度规则</SectionLabel>
        <div className="flex items-center gap-2 text-sm">
          <span>每</span>
          <Label htmlFor="interval-minutes" className="sr-only">
            运行间隔（分钟）
          </Label>
          <Input
            id="interval-minutes"
            type="number"
            min={15}
            max={240}
            disabled={busy}
            value={intervalText}
            onChange={(event) => setIntervalText(event.target.value)}
            className="h-8 w-20 text-center"
          />
          <span>分钟运行一次</span>
        </div>
        <p className="text-muted-foreground text-xs">
          按间隔自动生成工作日盘中时点（间隔最小 15 分钟）
        </p>
      </section>

      <section className="flex flex-col gap-3" aria-label="间隔任务运行时点预览">
        <SectionLabel icon={<Clock3Icon className="size-3.5" />}>
          运行时点预览
          <span className="text-muted-foreground/80 font-normal tabular-nums">
            {preview.length} 个
          </span>
        </SectionLabel>
        <div className="flex flex-wrap gap-2">
          {preview.map((time) => (
            <span
              key={time}
              className="border-border/60 bg-muted/40 text-foreground/80 inline-flex h-7 items-center rounded-lg border px-2.5 text-xs font-medium tabular-nums"
            >
              {time}
            </span>
          ))}
        </div>
      </section>

      <div className="border-border/60 flex justify-end border-t pt-4">
        <Button disabled={busy} onClick={() => void handleSave()}>
          {busy ? <Spinner className="size-4" /> : null}
          保存间隔任务设置
        </Button>
      </div>
    </ModeCardShell>
  );
}

/** Two mutually exclusive schedule modes: custom times or interval-derived. */
function ScheduleModeSection({
  schedule,
  savePending,
  writeDisabled,
  onSubmit,
}: {
  schedule?: StrategySchedule | undefined;
  savePending: boolean;
  writeDisabled: boolean;
  onSubmit: ScheduleSettingsCardsProps["onSubmit"];
}) {
  const busy = writeDisabled || savePending;
  const customActive = schedule
    ? schedule.enabled && Boolean(schedule.custom_schedule_times)
    : false;
  const intervalActive = schedule ? schedule.enabled && !schedule.custom_schedule_times : false;
  const [customOn, setCustomOn] = useState(customActive);
  const [intervalOn, setIntervalOn] = useState(intervalActive || !schedule);

  const toggleCustom = (on: boolean) => {
    if (on) {
      setIntervalOn(false);
    }
    setCustomOn(on);
  };
  const toggleInterval = (on: boolean) => {
    if (on) {
      setCustomOn(false);
    }
    setIntervalOn(on);
  };

  const handleDisable = async () => {
    if (!schedule) {
      return;
    }
    const payload = {
      enabled: false,
      task_type: "market_analysis" as const,
      interval_minutes: schedule.interval_minutes,
      // Keep any custom trigger times so re-enabling restores the same plan.
      schedule_times: schedule.custom_schedule_times ?? null,
      expected_revision: schedule.revision,
    };
    try {
      await onSubmit({ scheduleId: schedule.schedule_id, payload });
    } catch {
      // The page-level mutation owns error and conflict feedback.
    }
  };

  const alreadyDisabled = schedule !== undefined && !schedule.enabled;

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-col gap-4">
        <CustomTimeCard
          schedule={schedule}
          enabled={customOn}
          busy={busy}
          onSubmit={onSubmit}
          onToggle={toggleCustom}
        />
        <IntervalCard
          schedule={schedule}
          enabled={intervalOn}
          busy={busy}
          onSubmit={onSubmit}
          onToggle={toggleInterval}
        />
      </div>

      {schedule && !customOn && !intervalOn ? (
        <div className="border-border/60 bg-card/50 flex flex-wrap items-center justify-between gap-3 rounded-xl border px-4 py-3 sm:px-5">
          {alreadyDisabled ? (
            <span className="text-muted-foreground text-sm">任务已停用，不会自动运行</span>
          ) : (
            <>
              <span className="text-muted-foreground text-sm">
                当前未启用任何运行方式，任务将不会自动运行
              </span>
              <Button variant="outline" disabled={busy} onClick={() => void handleDisable()}>
                停用任务
              </Button>
            </>
          )}
        </div>
      ) : null}
    </div>
  );
}

export function ScheduleSettingsCards({
  schedules,
  savePending,
  writeDisabled,
  onSubmit,
}: ScheduleSettingsCardsProps) {
  const scheduleMap = useMemo(
    () => Object.fromEntries(schedules.map((schedule) => [schedule.task_type, schedule])),
    [schedules],
  );

  return (
    <div className="flex flex-col gap-6">
      <ScheduleModeSection
        key={`market-${scheduleMap.market_analysis?.updated_at ?? "new"}`}
        schedule={scheduleMap.market_analysis}
        savePending={savePending}
        writeDisabled={writeDisabled}
        onSubmit={onSubmit}
      />
    </div>
  );
}
