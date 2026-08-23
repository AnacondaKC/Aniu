import { normalizeModelBaseUrl } from "@/lib/model-endpoint";
import type { ThinkingEffort } from "@/lib/thinking-effort";
import type {
  ModelCatalogItem,
  ModelProfile,
  ModelProfilePayload,
  ModelProtocol,
  SelectedModelPayload,
} from "@/lib/api-types";

export type SelectedModelDraft = ModelCatalogItem & {
  context_window_tokens?: number | null;
  max_output_tokens?: number | null;
  input_price_per_million?: number | null;
  output_price_per_million?: number | null;
  cache_read_price_per_million?: number | null;
  cache_write_price_per_million?: number | null;
  thinking_efforts?: ThinkingEffort[];
};

/** Editor lifecycle for a single channel draft. */
type ChannelDraftStatus = "clean" | "dirty" | "saving" | "conflict" | "deleting";

export type ChannelDraft = {
  key: string;
  profileId: number | null;
  /** Last known server revision for this channel (null for unsaved). */
  revision: number | null;
  /** Revision the current draft content is based on. */
  baseRevision: number | null;
  /** Incremented for every persisted-field edit. */
  editVersion: number;
  status: ChannelDraftStatus;
  name: string;
  protocol: ModelProtocol;
  baseUrl: string;
  apiKey: string;
  providerConfig: ModelProfile["provider_config"];
  manualModelName: string;
  manualProviderId: string;
  manualContextWindowTokens: string;
  manualMaxOutputTokens: string;
  manualInputPrice: string;
  manualOutputPrice: string;
  manualCacheReadPrice: string;
  manualCacheWritePrice: string;
  manualThinkingEfforts: ThinkingEffort[];
  enabled: boolean;
  sortOrder: number;
  selectedModels: Map<string, SelectedModelDraft>;
  fetchedModels: ModelCatalogItem[];
  hasFetchedModels: boolean;
  fetchPending: boolean;
};

export const PROTOCOL_OPTIONS: Array<{
  value: ModelProtocol;
  label: string;
  placeholder: string;
}> = [
  {
    value: "openai_chat_completions",
    label: "OpenAI Chat Completions",
    placeholder: "https://api.openai.com/v1",
  },
  {
    value: "claude_api",
    label: "Claude API",
    placeholder: "https://api.anthropic.com/v1",
  },
];

export function protocolMeta(protocol: ModelProtocol) {
  return PROTOCOL_OPTIONS.find((option) => option.value === protocol) ?? PROTOCOL_OPTIONS[0]!;
}

function modelKey(item: Pick<ModelCatalogItem, "provider_id" | "model">) {
  return item.provider_id ?? item.model;
}

export function toChannelDraft(channel: ModelProfile): ChannelDraft {
  const selectedEntries = channel.selected_models.map((item) => {
    const selectedModel = {
      model: item.model_name,
      label: item.model_name,
      provider_id: item.provider_id ?? null,
      context_window_tokens: item.context_window_tokens ?? null,
      max_output_tokens: item.max_output_tokens ?? null,
      input_price_per_million: item.input_price_per_million ?? null,
      output_price_per_million: item.output_price_per_million ?? null,
      cache_read_price_per_million: item.cache_read_price_per_million ?? null,
      cache_write_price_per_million: item.cache_write_price_per_million ?? null,
      thinking_efforts: item.thinking_efforts ?? [],
    };
    return [
      modelKey({ provider_id: item.provider_id ?? null, model: item.model_name }),
      selectedModel,
    ] as const;
  });
  const selectedModels = new Map(selectedEntries);
  return {
    key: `channel-${channel.profile_id}`,
    profileId: channel.profile_id,
    revision: channel.revision,
    baseRevision: channel.revision,
    editVersion: 0,
    status: "clean",
    name: channel.name,
    protocol: channel.protocol,
    baseUrl: channel.base_url ?? "",
    // GET responses never include the raw key; empty means "keep current".
    apiKey: "",
    providerConfig: channel.provider_config,
    manualModelName: "",
    manualProviderId: "",
    manualContextWindowTokens: "",
    manualMaxOutputTokens: "",
    manualInputPrice: "",
    manualOutputPrice: "",
    manualCacheReadPrice: "",
    manualCacheWritePrice: "",
    manualThinkingEfforts: [],
    enabled: channel.enabled,
    sortOrder: channel.sort_order,
    selectedModels,
    fetchedModels: [],
    hasFetchedModels: false,
    fetchPending: false,
  };
}

let fallbackDraftKeyCounter = 0;

function createDraftKey(): string {
  const cryptoApi = globalThis.crypto;
  if (typeof cryptoApi?.randomUUID === "function") {
    return cryptoApi.randomUUID();
  }

  if (typeof cryptoApi?.getRandomValues === "function") {
    const bytes = cryptoApi.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6]! & 0x0f) | 0x40;
    bytes[8] = (bytes[8]! & 0x3f) | 0x80;
    const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }

  return `draft-${Date.now()}-${fallbackDraftKeyCounter++}-${Math.random().toString(36).slice(2)}`;
}

export function createEmptyChannelDraft(sortOrder: number): ChannelDraft {
  return {
    key: createDraftKey(),
    profileId: null,
    revision: null,
    baseRevision: null,
    editVersion: 0,
    status: "dirty",
    name: `渠道 ${sortOrder + 1}`,
    protocol: "openai_chat_completions",
    baseUrl: "",
    apiKey: "",
    providerConfig: { auth_mode: "auto" },
    manualModelName: "",
    manualProviderId: "",
    manualContextWindowTokens: "",
    manualMaxOutputTokens: "",
    manualInputPrice: "",
    manualOutputPrice: "",
    manualCacheReadPrice: "",
    manualCacheWritePrice: "",
    manualThinkingEfforts: [],
    enabled: true,
    sortOrder,
    selectedModels: new Map(),
    fetchedModels: [],
    hasFetchedModels: false,
    fetchPending: false,
  };
}

export function toChannelPayload(draft: ChannelDraft): ModelProfilePayload {
  const trimmedKey = draft.apiKey.trim();
  return {
    name: draft.name,
    protocol: draft.protocol,
    model_name: draft.selectedModels.values().next().value?.model ?? draft.name,
    base_url: normalizeModelBaseUrl(draft.protocol, draft.baseUrl),
    // Omit empty keys so the backend keeps the stored secret.
    api_key: trimmedKey.length > 0 ? trimmedKey : null,
    provider_config: draft.providerConfig,
    enabled: draft.enabled,
    sort_order: draft.sortOrder,
  };
}

export function toSelectedModelPayloads(draft: ChannelDraft): SelectedModelPayload[] {
  return [...draft.selectedModels.values()].map((item, index) => ({
    model_name: item.model,
    label: item.model,
    provider_id: item.provider_id ?? null,
    context_window_tokens: item.context_window_tokens ?? null,
    max_output_tokens: item.max_output_tokens ?? null,
    input_price_per_million: item.input_price_per_million ?? null,
    output_price_per_million: item.output_price_per_million ?? null,
    cache_read_price_per_million: item.cache_read_price_per_million ?? null,
    cache_write_price_per_million: item.cache_write_price_per_million ?? null,
    thinking_efforts: item.thinking_efforts ?? [],
    sort_order: index,
  }));
}

export function hasChannelEndpoint(draft: ChannelDraft) {
  return normalizeModelBaseUrl(draft.protocol, draft.baseUrl).trim().length > 0;
}

export function modelCatalogFingerprint(draft: ChannelDraft): string {
  return JSON.stringify([
    draft.protocol,
    normalizeModelBaseUrl(draft.protocol, draft.baseUrl),
    draft.apiKey.trim(),
    draft.providerConfig,
  ]);
}

export function canSaveChannel(draft: ChannelDraft) {
  return (
    draft.name.trim().length > 0 &&
    hasChannelEndpoint(draft) &&
    (draft.profileId === null || draft.revision !== null) &&
    draft.status !== "saving" &&
    draft.status !== "deleting" &&
    draft.status !== "conflict"
  );
}

function dirtyStatus(draft: ChannelDraft): ChannelDraftStatus {
  return draft.status === "conflict" ? "conflict" : "dirty";
}

/** Return a dirty draft with the catalog model added, or null when already present. */
export function withCatalogModel(
  draft: ChannelDraft,
  item: SelectedModelDraft,
): ChannelDraft | null {
  const key = modelKey(item);
  if (draft.selectedModels.has(key)) {
    return null;
  }
  const selectedModels = new Map(draft.selectedModels);
  selectedModels.set(key, {
    ...item,
    label: item.label || item.model,
    provider_id: item.provider_id ?? null,
  });
  return {
    ...draft,
    selectedModels,
    editVersion: draft.editVersion + 1,
    status: dirtyStatus(draft),
  };
}

/**
 * Return a draft after a manual model add.
 * - `null` when the input is empty
 * - `added: false` when the model already exists (name is still cleared)
 * - `added: true` when a new model was appended
 */
export function withManualModel(
  draft: ChannelDraft,
): { draft: ChannelDraft; added: boolean } | null {
  const modelName = draft.manualModelName.trim();
  if (!modelName || !manualModelIsValid(draft)) {
    return null;
  }
  const providerId = draft.manualProviderId.trim() || modelName;
  const key = modelKey({ provider_id: providerId, model: modelName });
  if (draft.selectedModels.has(key)) {
    return { draft: { ...draft, manualModelName: "" }, added: false };
  }
  const selectedModels = new Map(draft.selectedModels);
  selectedModels.set(key, {
    model: modelName,
    label: modelName,
    provider_id: providerId,
    context_window_tokens: optionalInteger(draft.manualContextWindowTokens),
    max_output_tokens: optionalInteger(draft.manualMaxOutputTokens),
    input_price_per_million: optionalNumber(draft.manualInputPrice),
    output_price_per_million: optionalNumber(draft.manualOutputPrice),
    cache_read_price_per_million: optionalNumber(draft.manualCacheReadPrice),
    cache_write_price_per_million: optionalNumber(draft.manualCacheWritePrice),
    thinking_efforts: draft.manualThinkingEfforts,
  });
  return {
    draft: {
      ...draft,
      manualModelName: "",
      manualProviderId: "",
      manualContextWindowTokens: "",
      manualMaxOutputTokens: "",
      manualInputPrice: "",
      manualOutputPrice: "",
      manualCacheReadPrice: "",
      manualCacheWritePrice: "",
      manualThinkingEfforts: [],
      selectedModels,
      editVersion: draft.editVersion + 1,
      status: dirtyStatus(draft),
    },
    added: true,
  };
}

/** Replace one selected model with the values currently entered in the editor. */
export function withUpdatedManualModel(
  draft: ChannelDraft,
  selectedModelKey: string,
): ChannelDraft | null {
  const previous = draft.selectedModels.get(selectedModelKey);
  const modelName = draft.manualModelName.trim();
  if (!previous || !modelName || !manualModelIsValid(draft)) {
    return null;
  }
  const enteredProviderId = draft.manualProviderId.trim();
  const providerId =
    modelName !== previous.model && enteredProviderId === previous.provider_id
      ? modelName
      : enteredProviderId || previous.provider_id || null;
  const nextKey = modelKey({ provider_id: providerId, model: modelName });
  if (nextKey !== selectedModelKey && draft.selectedModels.has(nextKey)) {
    return null;
  }
  const selectedModels = new Map(draft.selectedModels);
  selectedModels.delete(selectedModelKey);
  selectedModels.set(nextKey, {
    model: modelName,
    label: modelName === previous.model ? previous.label : modelName,
    provider_id: providerId,
    context_window_tokens: optionalInteger(draft.manualContextWindowTokens),
    max_output_tokens: optionalInteger(draft.manualMaxOutputTokens),
    input_price_per_million: optionalNumber(draft.manualInputPrice),
    output_price_per_million: optionalNumber(draft.manualOutputPrice),
    cache_read_price_per_million: optionalNumber(draft.manualCacheReadPrice),
    cache_write_price_per_million: optionalNumber(draft.manualCacheWritePrice),
    thinking_efforts: draft.manualThinkingEfforts,
  });
  return {
    ...draft,
    manualModelName: "",
    manualProviderId: "",
    manualContextWindowTokens: "",
    manualMaxOutputTokens: "",
    manualInputPrice: "",
    manualOutputPrice: "",
    manualCacheReadPrice: "",
    manualCacheWritePrice: "",
    manualThinkingEfforts: [],
    selectedModels,
    editVersion: draft.editVersion + 1,
    status: dirtyStatus(draft),
  };
}

function optionalInteger(value: string): number | null {
  if (!value.trim()) return null;
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : null;
}

function optionalNumber(value: string): number | null {
  return value.trim() ? Number(value) : null;
}

function isOptionalNumber(value: string, minimum: number) {
  if (!value.trim()) return true;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= minimum;
}

function isOptionalInteger(value: string, minimum: number) {
  if (!value.trim()) return true;
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= minimum;
}

function manualModelIsValid(draft: ChannelDraft) {
  return manualModelValidationMessage(draft) === null;
}

export function manualModelValidationMessage(draft: ChannelDraft): string | null {
  if (!draft.manualModelName.trim()) return "请输入模型名称";
  if (!isOptionalInteger(draft.manualContextWindowTokens, 1000)) {
    return "最大上下文 Token 必须是不小于 1000 的整数";
  }
  if (!isOptionalInteger(draft.manualMaxOutputTokens, 1)) {
    return "最大输出 Token 必须是正整数";
  }
  const context = optionalInteger(draft.manualContextWindowTokens);
  const output = optionalInteger(draft.manualMaxOutputTokens);
  if (context !== null && output !== null && output > context) {
    return "最大输出 Token 不能大于最大上下文 Token";
  }
  const hasInvalidPrice = [
    draft.manualInputPrice,
    draft.manualOutputPrice,
    draft.manualCacheReadPrice,
    draft.manualCacheWritePrice,
  ].some((value) => !isOptionalNumber(value, 0));
  return hasInvalidPrice ? "价格必须是大于或等于 0 的有效数字" : null;
}

export function formatModelPrice(value: number | null | undefined): string {
  if (value == null) return "";
  return Number(value.toFixed(8)).toString();
}

export function apiKeyPlaceholder(draft: ChannelDraft, channels: ModelProfile[]): string {
  if (draft.profileId == null) {
    return "输入模型 API 密钥";
  }
  const channel = channels.find((item) => item.profile_id === draft.profileId);
  if (!channel?.api_key_configured) {
    return "输入模型 API 密钥";
  }
  const suffix = channel.api_key_last_four ? `（尾号 ${channel.api_key_last_four}）` : "";
  return `已配置${suffix}，留空保持不变`;
}
