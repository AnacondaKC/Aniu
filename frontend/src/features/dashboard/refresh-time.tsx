import { formatMonthDayTime } from "@/lib/format";

type RefreshTimeProps = {
  value: string | null | undefined;
};

export function RefreshTime({ value }: RefreshTimeProps) {
  return (
    <span className="text-muted-foreground min-w-0 flex-1 truncate text-right text-xs tabular-nums">
      最近刷新：{formatMonthDayTime(value)}
    </span>
  );
}
