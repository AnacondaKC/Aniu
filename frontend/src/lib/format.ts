import { format } from "date-fns";

const runtimeTextReplacements: Array<[string, string]> = [
  ["run_is_active", "该运行仍在执行中"],
  ["market session closed before execution", "下单前已收盘，未执行交易"],
  ["market session closed during execution", "执行过程中已收盘，停止后续交易"],
  ["could not determine latest price for execution", "无法获取执行所需最新价"],
  ["execution retries exhausted before full fill", "达到最大重试次数后仍未全部成交"],
  ["strategy run aborted", "运行已中止"],
  ["manual_stop", "手动停止"],
];

const currencyFormatter = new Intl.NumberFormat("zh-CN", {
  style: "currency",
  currency: "CNY",
  maximumFractionDigits: 2,
});
const numberFormatter = new Intl.NumberFormat("zh-CN");

export function formatCurrency(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }

  return currencyFormatter.format(value);
}

export function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }

  return numberFormatter.format(value);
}

export function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }

  const percent = value * 100;
  const sign = percent > 0 ? "+" : "";
  return `${sign}${percent.toFixed(2)}%`;
}

export function formatDateOnly(value: string | null | undefined) {
  if (!value) {
    return "--";
  }

  return format(new Date(value), "yyyy-MM-dd");
}

export function formatMonthDayTime(value: string | null | undefined) {
  if (!value) {
    return "--";
  }

  return format(new Date(value), "MM-dd HH:mm");
}

export function formatTimeWithSeconds(value: string | null | undefined) {
  if (!value) {
    return "--";
  }

  return format(new Date(value), "HH:mm:ss");
}

export function formatRunDuration(
  startedAt: string | null | undefined,
  completedAt: string | null | undefined,
  now: Date = new Date(),
) {
  if (!startedAt) {
    return "--";
  }

  const started = new Date(startedAt).getTime();
  const ended = completedAt ? new Date(completedAt).getTime() : now.getTime();
  const diffMs = Math.max(0, ended - started);
  const totalSeconds = Math.floor(diffMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}小时${minutes}分钟`;
  }
  if (minutes > 0) {
    return `${minutes}分${seconds}秒`;
  }
  return `${seconds}秒`;
}

export function getErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return translateRuntimeText(error.message);
  }

  return "发生未知错误";
}

function translateRuntimeText(value: string) {
  return runtimeTextReplacements.reduce(
    (current, [source, target]) => current.replaceAll(source, target),
    value,
  );
}
