/** Local multi-stage prompt profiles. */

export type PromptProfileConfig = {
  name: string;
  global_prompt: string;
  run_prompt: string;
  summary_prompt: string;
  dream_prompt: string;
};

const PROMPT_CONFIGS_STORAGE_KEY = "aniu.prompt-configs.v3";
const LEGACY_PROMPT_CONFIGS_STORAGE_KEY = "aniu.prompt-configs.v2";
const REQUIRED_PROMPT_FIELDS = ["global_prompt", "run_prompt", "summary_prompt"] as const;

function normalizePromptConfig(item: unknown): PromptProfileConfig | null {
  if (typeof item !== "object" || item === null) return null;
  const data = item as Record<string, unknown>;
  if (
    typeof data.name !== "string" ||
    !REQUIRED_PROMPT_FIELDS.every((field) => typeof data[field] === "string")
  ) {
    return null;
  }
  return {
    name: data.name,
    global_prompt: String(data.global_prompt),
    run_prompt: String(data.run_prompt),
    summary_prompt: String(data.summary_prompt),
    dream_prompt: typeof data.dream_prompt === "string" ? data.dream_prompt : "",
  };
}

function readPromptConfigs(key: string): PromptProfileConfig[] | null {
  const raw = localStorage.getItem(key);
  if (!raw) return null;
  const parsed: unknown = JSON.parse(raw);
  return Array.isArray(parsed)
    ? parsed.flatMap((item) => {
        const normalized = normalizePromptConfig(item);
        return normalized === null ? [] : [normalized];
      })
    : [];
}

export function listPromptConfigs(): PromptProfileConfig[] {
  try {
    const current = readPromptConfigs(PROMPT_CONFIGS_STORAGE_KEY);
    if (current !== null) return current;
    const legacy = readPromptConfigs(LEGACY_PROMPT_CONFIGS_STORAGE_KEY) ?? [];
    if (legacy.length > 0) {
      localStorage.setItem(PROMPT_CONFIGS_STORAGE_KEY, JSON.stringify(legacy));
    }
    return legacy;
  } catch {
    return [];
  }
}

export function savePromptConfig(config: PromptProfileConfig): void {
  const current = listPromptConfigs();
  const next = [...current.filter((item) => item.name !== config.name), config];
  localStorage.setItem(PROMPT_CONFIGS_STORAGE_KEY, JSON.stringify(next));
}

export function parseImportedConfig(raw: unknown): PromptProfileConfig {
  if (typeof raw !== "object" || raw === null) {
    throw new Error("文件内容不是有效的提示词配置");
  }
  const data = raw as Record<string, unknown>;
  const name = typeof data.name === "string" && data.name.trim() ? data.name.trim() : "导入配置";
  const missing = REQUIRED_PROMPT_FIELDS.filter((field) => typeof data[field] !== "string");
  if (missing.length > 0) {
    throw new Error(`缺少字段：${missing.join("、")}`);
  }
  return {
    name,
    global_prompt: String(data.global_prompt),
    run_prompt: String(data.run_prompt),
    summary_prompt: String(data.summary_prompt),
    dream_prompt: typeof data.dream_prompt === "string" ? data.dream_prompt : "",
  };
}

export function downloadConfig(config: PromptProfileConfig): void {
  const blob = new Blob([JSON.stringify(config, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `提示词配置-${config.name}-${new Date().toISOString().slice(0, 10)}.json`;
  anchor.click();
  URL.revokeObjectURL(url);
}
