export const THINKING_EFFORT_OPTIONS = [
  { value: "minimal", label: "最低", englishLabel: "Minimal" },
  { value: "low", label: "低", englishLabel: "Low" },
  { value: "medium", label: "中", englishLabel: "Medium" },
  { value: "high", label: "高", englishLabel: "High" },
  { value: "xhigh", label: "极高", englishLabel: "Extra High" },
  { value: "max", label: "最大", englishLabel: "Max" },
] as const;

export type ThinkingEffort = (typeof THINKING_EFFORT_OPTIONS)[number]["value"];

export const DEFAULT_THINKING_EFFORT_VALUE = "model-default";

export function isThinkingEffort(value: string): value is ThinkingEffort {
  return THINKING_EFFORT_OPTIONS.some((option) => option.value === value);
}

export function thinkingEffortLabel(value: ThinkingEffort): string {
  return THINKING_EFFORT_OPTIONS.find((option) => option.value === value)?.label ?? value;
}
