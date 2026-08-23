import type { ModelCatalogItem, ModelProfile } from "@/lib/api-types";

import {
  createEmptyChannelDraft,
  formatModelPrice,
  modelCatalogFingerprint,
  toChannelDraft,
  withCatalogModel,
  withManualModel,
  withUpdatedManualModel,
  type ChannelDraft,
} from "./channel-draft";

export type ChannelEditorState = {
  drafts: ChannelDraft[];
  expandedKeys: string[];
  modelManager: { key: string; editingModelKey: string | null } | null;
};

type DraftPatch = Partial<
  Pick<
    ChannelDraft,
    | "name"
    | "protocol"
    | "baseUrl"
    | "apiKey"
    | "manualModelName"
    | "manualProviderId"
    | "manualContextWindowTokens"
    | "manualMaxOutputTokens"
    | "manualInputPrice"
    | "manualOutputPrice"
    | "manualCacheReadPrice"
    | "manualCacheWritePrice"
    | "manualThinkingEfforts"
    | "enabled"
    | "sortOrder"
    | "selectedModels"
    | "fetchedModels"
    | "hasFetchedModels"
    | "fetchPending"
  >
>;

export type ChannelEditorAction =
  | { type: "sync_channels"; channels: ModelProfile[] }
  | { type: "add_channel" }
  | { type: "patch_draft"; key: string; patch: DraftPatch; markDirty?: boolean }
  | { type: "normalize_base_url"; key: string; baseUrl: string }
  | { type: "add_selected_model"; key: string; item: ModelCatalogItem }
  | { type: "remove_selected_model"; key: string; modelKey: string }
  | { type: "add_manual_model"; key: string }
  | { type: "update_manual_model"; key: string; modelKey: string }
  | { type: "set_expanded_keys"; keys: string[] }
  | { type: "open_model_manager"; key: string; modelKey?: string }
  | { type: "close_model_manager" }
  | { type: "mark_saving"; key: string }
  | {
      type: "mark_saved";
      key: string;
      channel: ModelProfile;
      selectedModels: Map<string, ModelCatalogItem>;
      savedEditVersion: number;
    }
  | { type: "mark_save_failed"; key: string }
  | { type: "mark_deleting"; key: string }
  | { type: "mark_delete_failed"; key: string }
  | { type: "remove_draft"; key: string }
  | { type: "clear_api_key_local"; key: string }
  | { type: "fetch_models_start"; key: string; fingerprint: string }
  | {
      type: "fetch_models_success";
      key: string;
      fingerprint: string;
      items: ModelCatalogItem[];
    }
  | { type: "fetch_models_failed"; key: string; fingerprint: string };

export function createInitialChannelEditorState(channels: ModelProfile[]): ChannelEditorState {
  return {
    drafts: channels.map(toChannelDraft),
    expandedKeys: [],
    modelManager: null,
  };
}

function updateStateDraft(
  state: ChannelEditorState,
  key: string,
  update: (draft: ChannelDraft) => ChannelDraft,
): ChannelEditorState {
  return {
    ...state,
    drafts: state.drafts.map((draft) => (draft.key === key ? update(draft) : draft)),
  };
}

function dirtyStatus(draft: ChannelDraft) {
  return draft.status === "conflict" ? ("conflict" as const) : ("dirty" as const);
}

function mergeServerChannels(current: ChannelDraft[], channels: ModelProfile[]): ChannelDraft[] {
  const savedDraftsById = new Map(
    current
      .filter((draft) => draft.profileId !== null)
      .map((draft) => [draft.profileId as number, draft]),
  );
  const unsaved = current.filter((draft) => draft.profileId === null);

  return [
    ...channels.map((channel) => {
      const serverDraft = toChannelDraft(channel);
      const existing = savedDraftsById.get(channel.profile_id);
      if (existing === undefined) return serverDraft;
      if (existing.status === "clean") {
        return {
          ...serverDraft,
          manualModelName: existing.manualModelName,
          fetchedModels: existing.fetchedModels,
          hasFetchedModels: existing.hasFetchedModels,
          fetchPending: existing.fetchPending,
        };
      }

      const serverAdvanced =
        existing.baseRevision !== null && channel.revision !== existing.baseRevision;
      const status =
        existing.status === "saving" || existing.status === "deleting"
          ? existing.status
          : serverAdvanced
            ? "conflict"
            : existing.status;
      return {
        ...existing,
        key: serverDraft.key,
        profileId: serverDraft.profileId,
        revision: serverDraft.revision,
        enabled: serverDraft.enabled,
        sortOrder: serverDraft.sortOrder,
        status,
      };
    }),
    ...unsaved,
  ];
}

export function channelEditorReducer(
  state: ChannelEditorState,
  action: ChannelEditorAction,
): ChannelEditorState {
  switch (action.type) {
    case "sync_channels":
      return { ...state, drafts: mergeServerChannels(state.drafts, action.channels) };

    case "add_channel": {
      const draft = createEmptyChannelDraft(state.drafts.length);
      return {
        ...state,
        drafts: [...state.drafts, draft],
        expandedKeys: [...state.expandedKeys, draft.key],
      };
    }

    case "patch_draft":
      return updateStateDraft(state, action.key, (draft) => {
        const invalidatesCatalog =
          "protocol" in action.patch || "baseUrl" in action.patch || "apiKey" in action.patch;
        return {
          ...draft,
          ...action.patch,
          editVersion: action.markDirty ? draft.editVersion + 1 : draft.editVersion,
          status: action.markDirty ? dirtyStatus(draft) : draft.status,
          ...(invalidatesCatalog
            ? {
                fetchedModels: [],
                hasFetchedModels: false,
                fetchPending: false,
              }
            : {}),
        };
      });

    case "normalize_base_url":
      return updateStateDraft(state, action.key, (draft) =>
        draft.baseUrl === action.baseUrl
          ? draft
          : {
              ...draft,
              baseUrl: action.baseUrl,
              editVersion: draft.editVersion + 1,
              status: dirtyStatus(draft),
              fetchedModels: [],
              hasFetchedModels: false,
              fetchPending: false,
            },
      );

    case "add_selected_model":
      return updateStateDraft(state, action.key, (draft) => {
        return withCatalogModel(draft, action.item) ?? draft;
      });

    case "remove_selected_model":
      return updateStateDraft(state, action.key, (draft) => {
        if (!draft.selectedModels.has(action.modelKey)) return draft;
        const selectedModels = new Map(draft.selectedModels);
        selectedModels.delete(action.modelKey);
        return {
          ...draft,
          selectedModels,
          editVersion: draft.editVersion + 1,
          status: dirtyStatus(draft),
        };
      });

    case "add_manual_model":
      return updateStateDraft(state, action.key, (draft) => {
        return withManualModel(draft)?.draft ?? draft;
      });

    case "update_manual_model":
      return updateStateDraft(state, action.key, (draft) => {
        return withUpdatedManualModel(draft, action.modelKey) ?? draft;
      });

    case "set_expanded_keys":
      return { ...state, expandedKeys: action.keys };
    case "open_model_manager": {
      const editingModelKey = action.modelKey ?? null;
      return {
        ...updateStateDraft(state, action.key, (draft) => {
          const model = editingModelKey ? draft.selectedModels.get(editingModelKey) : null;
          return {
            ...draft,
            manualModelName: model?.model ?? "",
            manualProviderId: model?.provider_id ?? "",
            manualContextWindowTokens:
              model?.context_window_tokens == null ? "" : String(model.context_window_tokens),
            manualMaxOutputTokens:
              model?.max_output_tokens == null ? "" : String(model.max_output_tokens),
            manualInputPrice: formatModelPrice(model?.input_price_per_million),
            manualOutputPrice: formatModelPrice(model?.output_price_per_million),
            manualCacheReadPrice: formatModelPrice(model?.cache_read_price_per_million),
            manualCacheWritePrice: formatModelPrice(model?.cache_write_price_per_million),
            manualThinkingEfforts: [...(model?.thinking_efforts ?? [])],
          };
        }),
        modelManager: { key: action.key, editingModelKey },
      };
    }
    case "close_model_manager":
      return { ...state, modelManager: null };

    case "mark_saving":
      return updateStateDraft(state, action.key, (draft) => ({
        ...draft,
        status: "saving",
      }));

    case "mark_saved": {
      const nextKey = `channel-${action.channel.profile_id}`;
      const next = updateStateDraft(state, action.key, (draft) => {
        const unchangedSinceSave = draft.editVersion === action.savedEditVersion;
        return {
          ...draft,
          key: nextKey,
          profileId: action.channel.profile_id,
          revision: action.channel.revision,
          baseRevision: action.channel.revision,
          selectedModels: unchangedSinceSave
            ? new Map(action.selectedModels)
            : draft.selectedModels,
          providerConfig: unchangedSinceSave
            ? action.channel.provider_config
            : draft.providerConfig,
          status: unchangedSinceSave ? "clean" : "dirty",
          apiKey: unchangedSinceSave ? "" : draft.apiKey,
        };
      });
      return {
        ...next,
        expandedKeys: state.expandedKeys.map((key) => (key === action.key ? nextKey : key)),
        modelManager:
          state.modelManager?.key === action.key
            ? { ...state.modelManager, key: nextKey }
            : state.modelManager,
      };
    }

    case "mark_save_failed":
      return updateStateDraft(state, action.key, (draft) => ({
        ...draft,
        status: dirtyStatus(draft),
      }));
    case "mark_deleting":
      return updateStateDraft(state, action.key, (draft) => ({
        ...draft,
        status: "deleting",
      }));
    case "mark_delete_failed":
      return updateStateDraft(state, action.key, (draft) => ({
        ...draft,
        status: draft.baseRevision === draft.revision ? "clean" : "dirty",
      }));

    case "remove_draft":
      return {
        drafts: state.drafts.filter((draft) => draft.key !== action.key),
        expandedKeys: state.expandedKeys.filter((key) => key !== action.key),
        modelManager: state.modelManager?.key === action.key ? null : state.modelManager,
      };
    case "clear_api_key_local":
      return updateStateDraft(state, action.key, (draft) => ({ ...draft, apiKey: "" }));
    case "fetch_models_start":
      return updateStateDraft(state, action.key, (draft) =>
        action.fingerprint === modelCatalogFingerprint(draft)
          ? { ...draft, fetchPending: true }
          : draft,
      );
    case "fetch_models_success":
      return updateStateDraft(state, action.key, (draft) =>
        action.fingerprint === modelCatalogFingerprint(draft)
          ? {
              ...draft,
              fetchPending: false,
              fetchedModels: action.items,
              hasFetchedModels: true,
            }
          : draft,
      );
    case "fetch_models_failed":
      return updateStateDraft(state, action.key, (draft) =>
        action.fingerprint === modelCatalogFingerprint(draft)
          ? { ...draft, fetchPending: false }
          : draft,
      );
    default:
      return state;
  }
}
