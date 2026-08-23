import { describe, expect, it } from "vitest";

import type { ModelProfile } from "@/lib/api-types";

import { channelEditorReducer, createInitialChannelEditorState } from "./channel-editor-reducer";
import { modelCatalogFingerprint, toChannelDraft } from "./channel-draft";

const channel: ModelProfile = {
  profile_id: 7,
  revision: 12,
  name: "Primary",
  protocol: "openai_chat_completions",
  model_name: "gpt-4.1",
  base_url: "https://api.example.test/v1",
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
      sort_order: 0,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-02T00:00:00Z",
    },
  ],
  provider_config: { auth_mode: "auto" },
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-02T00:00:00Z",
};

describe("channelEditorReducer", () => {
  it("initializes clean drafts from server channels", () => {
    const state = createInitialChannelEditorState([channel]);
    expect(state.drafts).toHaveLength(1);
    expect(state.drafts[0]).toMatchObject({
      profileId: 7,
      revision: 12,
      baseRevision: 12,
      status: "clean",
    });
  });

  it("marks a draft dirty when fields change", () => {
    const state = createInitialChannelEditorState([channel]);
    const next = channelEditorReducer(state, {
      type: "patch_draft",
      key: state.drafts[0]!.key,
      patch: { name: "Renamed" },
      markDirty: true,
    });
    expect(next.drafts[0]?.status).toBe("dirty");
    expect(next.drafts[0]?.name).toBe("Renamed");
  });

  it("auto-rebases clean drafts on server refresh", () => {
    const state = createInitialChannelEditorState([channel]);
    const refreshed = {
      ...channel,
      revision: 13,
      name: "Server Name",
    };
    const next = channelEditorReducer(state, {
      type: "sync_channels",
      channels: [refreshed],
    });
    expect(next.drafts[0]).toMatchObject({
      status: "clean",
      revision: 13,
      baseRevision: 13,
      name: "Server Name",
    });
  });

  it("enters conflict when server advances while draft is dirty", () => {
    let state = createInitialChannelEditorState([channel]);
    state = channelEditorReducer(state, {
      type: "patch_draft",
      key: state.drafts[0]!.key,
      patch: { name: "Local edit" },
      markDirty: true,
    });
    const next = channelEditorReducer(state, {
      type: "sync_channels",
      channels: [{ ...channel, revision: 13, name: "Server Name" }],
    });
    expect(next.drafts[0]).toMatchObject({
      status: "conflict",
      name: "Local edit",
      revision: 13,
    });
  });

  it("keeps local input after save failure", () => {
    let state = createInitialChannelEditorState([channel]);
    state = channelEditorReducer(state, {
      type: "patch_draft",
      key: state.drafts[0]!.key,
      patch: { name: "Local edit" },
      markDirty: true,
    });
    state = channelEditorReducer(state, {
      type: "mark_saving",
      key: state.drafts[0]!.key,
    });
    const next = channelEditorReducer(state, {
      type: "mark_save_failed",
      key: state.drafts[0]!.key,
    });
    expect(next.drafts[0]).toMatchObject({
      status: "dirty",
      name: "Local edit",
    });
  });

  it("establishes a new baseline after successful save", () => {
    let state = createInitialChannelEditorState([channel]);
    state = channelEditorReducer(state, {
      type: "patch_draft",
      key: state.drafts[0]!.key,
      patch: { name: "Saved name" },
      markDirty: true,
    });
    state = channelEditorReducer(state, {
      type: "mark_saving",
      key: state.drafts[0]!.key,
    });
    const selected = new Map(state.drafts[0]!.selectedModels);
    const next = channelEditorReducer(state, {
      type: "mark_saved",
      key: state.drafts[0]!.key,
      channel: { ...channel, revision: 14, name: "Saved name" },
      selectedModels: selected,
      savedEditVersion: state.drafts[0]!.editVersion,
    });
    expect(next.drafts[0]).toMatchObject({
      key: "channel-7",
      status: "clean",
      revision: 14,
      baseRevision: 14,
      name: "Saved name",
    });
  });

  it("adds and removes selected models as dirty edits", () => {
    let state = createInitialChannelEditorState([channel]);
    const key = state.drafts[0]!.key;
    state = channelEditorReducer(state, {
      type: "add_selected_model",
      key,
      item: { model: "gpt-mini", label: "gpt-mini", provider_id: "mini" },
    });
    expect(state.drafts[0]?.selectedModels.has("mini")).toBe(true);
    expect(state.drafts[0]?.status).toBe("dirty");

    state = channelEditorReducer(state, {
      type: "remove_selected_model",
      key,
      modelKey: "mini",
    });
    expect(state.drafts[0]?.selectedModels.has("mini")).toBe(false);
  });

  it("prefills and updates one selected model through the model manager", () => {
    let state = createInitialChannelEditorState([channel]);
    const key = state.drafts[0]!.key;
    state = channelEditorReducer(state, {
      type: "open_model_manager",
      key,
      modelKey: "openai/gpt-4.1",
    });
    expect(state.modelManager).toEqual({ key, editingModelKey: "openai/gpt-4.1" });
    expect(state.drafts[0]).toMatchObject({
      manualModelName: "gpt-4.1",
      manualProviderId: "openai/gpt-4.1",
    });

    state = channelEditorReducer(state, {
      type: "patch_draft",
      key,
      patch: { manualInputPrice: "2" },
    });
    state = channelEditorReducer(state, {
      type: "update_manual_model",
      key,
      modelKey: "openai/gpt-4.1",
    });
    expect(state.drafts[0]?.selectedModels).toHaveLength(1);
    expect(state.drafts[0]?.selectedModels.get("openai/gpt-4.1")).toMatchObject({
      input_price_per_million: 2,
    });
  });

  it("removes unsaved drafts without calling the server", () => {
    let state = createInitialChannelEditorState([]);
    state = channelEditorReducer(state, { type: "add_channel" });
    const key = state.drafts[0]!.key;
    expect(state.drafts[0]?.profileId).toBeNull();
    const next = channelEditorReducer(state, { type: "remove_draft", key });
    expect(next.drafts).toHaveLength(0);
  });

  it("keeps edits made while an older save is pending", () => {
    let state = createInitialChannelEditorState([channel]);
    const key = state.drafts[0]!.key;
    state = channelEditorReducer(state, {
      type: "patch_draft",
      key,
      patch: { name: "Submitted name" },
      markDirty: true,
    });
    const savedEditVersion = state.drafts[0]!.editVersion;
    state = channelEditorReducer(state, { type: "mark_saving", key });
    state = channelEditorReducer(state, {
      type: "patch_draft",
      key,
      patch: { name: "Newer local name" },
      markDirty: true,
    });

    const next = channelEditorReducer(state, {
      type: "mark_saved",
      key,
      channel: { ...channel, revision: 14, name: "Submitted name" },
      selectedModels: new Map(
        channel.selected_models.map((item) => [
          item.provider_id!,
          {
            model: item.model_name,
            label: item.label,
            provider_id: item.provider_id ?? null,
          },
        ]),
      ),
      savedEditVersion,
    });

    expect(next.drafts[0]).toMatchObject({
      name: "Newer local name",
      revision: 14,
      baseRevision: 14,
      status: "dirty",
    });
  });

  it("ignores model catalog responses for stale connection parameters", () => {
    let state = createInitialChannelEditorState([channel]);
    const key = state.drafts[0]!.key;
    const fingerprintA = modelCatalogFingerprint(state.drafts[0]!);
    state = channelEditorReducer(state, {
      type: "fetch_models_start",
      key,
      fingerprint: fingerprintA,
    });
    state = channelEditorReducer(state, {
      type: "patch_draft",
      key,
      patch: { baseUrl: "https://new.example.test/v1" },
      markDirty: true,
    });
    const fingerprintB = modelCatalogFingerprint(state.drafts[0]!);
    state = channelEditorReducer(state, {
      type: "fetch_models_start",
      key,
      fingerprint: fingerprintB,
    });
    state = channelEditorReducer(state, {
      type: "fetch_models_success",
      key,
      fingerprint: fingerprintA,
      items: [{ model: "old", label: "old", provider_id: "old" }],
    });
    expect(state.drafts[0]).toMatchObject({ fetchPending: true, fetchedModels: [] });

    state = channelEditorReducer(state, {
      type: "fetch_models_success",
      key,
      fingerprint: fingerprintB,
      items: [{ model: "new", label: "new", provider_id: "new" }],
    });
    expect(state.drafts[0]?.fetchPending).toBe(false);
    expect(state.drafts[0]?.fetchedModels.map((item) => item.model)).toEqual(["new"]);
  });

  it("preserves fetched model cache across clean rebase", () => {
    let state = createInitialChannelEditorState([channel]);
    const key = state.drafts[0]!.key;
    state = channelEditorReducer(state, {
      type: "fetch_models_success",
      key,
      fingerprint: modelCatalogFingerprint(state.drafts[0]!),
      items: [{ model: "a", label: "a", provider_id: null }],
    });
    const next = channelEditorReducer(state, {
      type: "sync_channels",
      channels: [{ ...channel, revision: 13 }],
    });
    expect(next.drafts[0]?.hasFetchedModels).toBe(true);
    expect(next.drafts[0]?.fetchedModels).toHaveLength(1);
  });

  it("toChannelDraft baseline matches reducer init", () => {
    expect(toChannelDraft(channel).status).toBe("clean");
  });
});
