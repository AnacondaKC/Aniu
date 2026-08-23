import { describe, expect, it, vi } from "vitest";

import type { ModelProfile } from "@/lib/api-types";
import type { ThinkingEffort } from "@/lib/thinking-effort";

import {
  createEmptyChannelDraft,
  canSaveChannel,
  formatModelPrice,
  manualModelValidationMessage,
  toChannelDraft,
  toChannelPayload,
  toSelectedModelPayloads,
  withCatalogModel,
  withManualModel,
  withUpdatedManualModel,
} from "./channel-draft";

const channel: ModelProfile = {
  profile_id: 7,
  revision: 12,
  name: "Primary",
  protocol: "openai_chat_completions",
  model_name: "gpt-4.1",
  base_url: "https://api.example.test/v1/chat/completions/",
  api_key_configured: true,
  api_key_last_four: "1234",
  enabled: true,
  sort_order: 2,
  selected_models: [
    {
      selected_model_id: 19,
      channel_profile_id: 7,
      model_name: "gpt-4.1",
      label: "GPT 4.1",
      provider_id: "openai/gpt-4.1",
      thinking_efforts: ["low", "high"],
      sort_order: 0,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-02T00:00:00Z",
    },
    {
      selected_model_id: 20,
      channel_profile_id: 7,
      model_name: "gpt-4.1-mini",
      label: "GPT 4.1 Mini",
      provider_id: null,
      sort_order: 1,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-02T00:00:00Z",
    },
  ],
  provider_config: { auth_mode: "auto" },
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-02T00:00:00Z",
};

describe("channel draft transformations", () => {
  it("creates a draft when randomUUID is unavailable", () => {
    const originalCrypto = globalThis.crypto;
    vi.stubGlobal("crypto", {
      getRandomValues: (bytes: Uint8Array) => bytes.fill(0),
    });

    try {
      expect(createEmptyChannelDraft(0).key).toBe("00000000-0000-4000-8000-000000000000");
    } finally {
      vi.stubGlobal("crypto", originalCrypto);
    }
  });
  it("preserves the revision and selected-model identity in the editable draft", () => {
    const draft = toChannelDraft(channel);

    expect(draft).toMatchObject({
      key: "channel-7",
      profileId: 7,
      revision: 12,
      baseRevision: 12,
      status: "clean",
      apiKey: "",
    });
    expect([...draft.selectedModels.keys()]).toEqual(["openai/gpt-4.1", "gpt-4.1-mini"]);
  });

  it("appends catalog models and prepares a dirty draft for auto-save", () => {
    const draft = toChannelDraft(channel);
    const next = withCatalogModel(draft, {
      model: "gpt-mini",
      label: "gpt-mini",
      provider_id: "mini",
    });
    expect(next?.status).toBe("dirty");
    expect(next?.selectedModels.has("mini")).toBe(true);
    expect(
      withCatalogModel(next!, {
        model: "gpt-mini",
        label: "gpt-mini",
        provider_id: "mini",
      }),
    ).toBeNull();
  });

  it("adds manual models and clears the input for auto-save", () => {
    const draft = {
      ...toChannelDraft(channel),
      manualModelName: " local-model ",
      manualProviderId: "local/local-model",
      manualContextWindowTokens: "2e5",
      manualMaxOutputTokens: "3.2e4",
      manualInputPrice: "1.25",
      manualOutputPrice: "5",
      manualCacheReadPrice: "0",
      manualCacheWritePrice: "1.5",
      manualThinkingEfforts: ["low", "high"] as ThinkingEffort[],
    };
    const result = withManualModel(draft);
    expect(result).toEqual({
      added: true,
      draft: expect.objectContaining({
        manualModelName: "",
        status: "dirty",
      }),
    });
    expect(result?.draft.selectedModels.has("local/local-model")).toBe(true);
    expect(result?.draft.selectedModels.get("local/local-model")).toMatchObject({
      context_window_tokens: 200000,
      max_output_tokens: 32000,
      input_price_per_million: 1.25,
      output_price_per_million: 5,
      cache_read_price_per_million: 0,
      cache_write_price_per_million: 1.5,
      thinking_efforts: ["low", "high"],
    });
  });

  it("replaces an existing model instead of adding a second model", () => {
    const draft = {
      ...toChannelDraft(channel),
      manualModelName: "gpt-4.1",
      manualProviderId: "openai/gpt-4.1",
      manualContextWindowTokens: "1000000",
      manualMaxOutputTokens: "32768",
      manualInputPrice: "2",
      manualOutputPrice: "8",
      manualCacheReadPrice: "0.2",
      manualCacheWritePrice: "2",
    };
    const next = withUpdatedManualModel(draft, "openai/gpt-4.1");

    expect(next?.selectedModels).toHaveLength(2);
    expect(next?.selectedModels.get("openai/gpt-4.1")).toMatchObject({
      context_window_tokens: 1_000_000,
      max_output_tokens: 32_768,
      input_price_per_million: 2,
      output_price_per_million: 8,
      cache_read_price_per_million: 0.2,
      cache_write_price_per_million: 2,
    });
  });

  it("rounds database float noise for editable price values", () => {
    expect(formatModelPrice(0.14000000059604645)).toBe("0.14");
    expect(formatModelPrice(0.00279999990016222)).toBe("0.0028");
  });

  it("explains invalid model limits instead of silently disabling an edit", () => {
    const draft = {
      ...toChannelDraft(channel),
      manualModelName: "gpt-4.1",
      manualContextWindowTokens: "1000",
      manualMaxOutputTokens: "2000",
    };

    expect(manualModelValidationMessage(draft)).toBe("最大输出 Token 不能大于最大上下文 Token");
  });

  it("builds strict create fields and normalizes an endpoint without resending an empty secret", () => {
    const draft = toChannelDraft(channel);

    expect(toChannelPayload(draft)).toEqual({
      name: "Primary",
      protocol: "openai_chat_completions",
      model_name: "gpt-4.1",
      base_url: "https://api.example.test/v1",
      api_key: null,
      provider_config: { auth_mode: "auto" },
      enabled: true,
      sort_order: 2,
    });
  });

  it("uses stable insertion order when building selected model payloads", () => {
    expect(toSelectedModelPayloads(toChannelDraft(channel))).toEqual([
      {
        model_name: "gpt-4.1",
        label: "gpt-4.1",
        provider_id: "openai/gpt-4.1",
        context_window_tokens: null,
        max_output_tokens: null,
        input_price_per_million: null,
        output_price_per_million: null,
        cache_read_price_per_million: null,
        cache_write_price_per_million: null,
        thinking_efforts: ["low", "high"],
        sort_order: 0,
      },
      {
        model_name: "gpt-4.1-mini",
        label: "gpt-4.1-mini",
        provider_id: null,
        context_window_tokens: null,
        max_output_tokens: null,
        input_price_per_million: null,
        output_price_per_million: null,
        cache_read_price_per_million: null,
        cache_write_price_per_million: null,
        thinking_efforts: [],
        sort_order: 1,
      },
    ]);
  });

  it("disables save while the draft is in conflict", () => {
    const draft = { ...toChannelDraft(channel), status: "conflict" as const };
    expect(canSaveChannel(draft)).toBe(false);
  });
});
