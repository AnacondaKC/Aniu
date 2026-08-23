import { useEffect, useReducer, useRef } from "react";
import {
  CheckCircle2Icon,
  CircleDashedIcon,
  KeyRoundIcon,
  NetworkIcon,
  PencilLineIcon,
  PlusIcon,
  TriangleAlertIcon,
} from "lucide-react";
import { toast } from "sonner";

import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import type {
  ModelCatalogFetchPayload,
  ModelCatalogItem,
  ModelProfile,
  ModelProfilePayload,
  ModelsDevModel,
  SelectedModelPayload,
} from "@/lib/api-types";
import { normalizeModelBaseUrl } from "@/lib/model-endpoint";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { Spinner } from "@/components/ui/spinner";
import { cn } from "@/lib/utils";

import {
  canSaveChannel,
  modelCatalogFingerprint,
  manualModelValidationMessage,
  protocolMeta,
  toChannelPayload,
  toSelectedModelPayloads,
  withManualModel,
  withUpdatedManualModel,
  type ChannelDraft,
} from "./channel-draft";
import { channelEditorReducer, createInitialChannelEditorState } from "./channel-editor-reducer";
import { ChannelForm } from "./channel-form";
import { ModelPickerDialog } from "./model-picker-dialog";

const statusBadgeClass =
  "h-5 gap-1 rounded-full border px-2 text-[11px] font-medium leading-none [&>svg]:size-3";

/** Save-lifecycle badge keeps channel persistence state visible beside its name. */
function ChannelStatusBadge({ draft }: { draft: ChannelDraft }) {
  if (draft.status === "deleting") {
    return (
      <Badge
        key="deleting"
        variant="outline"
        className={cn(statusBadgeClass, "border-border bg-muted/60 text-muted-foreground")}
      >
        <Spinner className="size-3" />
        删除中
      </Badge>
    );
  }
  if (draft.status === "saving") {
    return (
      <Badge
        key="saving"
        variant="outline"
        className={cn(statusBadgeClass, "border-sky-500/30 bg-sky-500/10 text-sky-700")}
      >
        <Spinner className="size-3" />
        保存中
      </Badge>
    );
  }
  if (draft.status === "conflict") {
    return (
      <Badge
        key="conflict"
        variant="outline"
        className={cn(statusBadgeClass, "border-destructive/30 bg-destructive/10 text-destructive")}
      >
        <TriangleAlertIcon />
        冲突
      </Badge>
    );
  }
  if (draft.profileId === null || draft.status === "dirty") {
    return (
      <Badge
        key={draft.profileId === null ? "unsaved" : "dirty"}
        variant="outline"
        className={cn(statusBadgeClass, "border-amber-500/30 bg-amber-500/10 text-amber-700")}
      >
        {draft.profileId === null ? <CircleDashedIcon /> : <PencilLineIcon />}
        {draft.profileId === null ? "未保存" : "未保存更改"}
      </Badge>
    );
  }
  return (
    <Badge
      key="saved"
      variant="outline"
      className={cn(statusBadgeClass, "border-emerald-500/25 bg-emerald-500/10 text-emerald-700")}
    >
      <CheckCircle2Icon />
      已保存
    </Badge>
  );
}

function MetaDot() {
  return <span aria-hidden="true" className="bg-border inline-block size-1 rounded-full" />;
}

type ModelSettingsCardProps = {
  channels: ModelProfile[];
  writeDisabled?: boolean;
  onSaveChannel: (
    channelId: number | null,
    channelPayload: ModelProfilePayload,
    selectedModelsPayload: SelectedModelPayload[],
    expectedRevision: number | null,
  ) => Promise<ModelProfile>;
  onDeleteChannel: (channelId: number) => Promise<void>;
  onClearApiKey: (channelId: number, revision: number) => Promise<ModelProfile>;
  onFetchModels: (
    channelId: number,
    payload: ModelCatalogFetchPayload,
  ) => Promise<ModelCatalogItem[]>;
  onLookupModelsDev: (modelName: string) => Promise<ModelsDevModel>;
};

export function ModelSettingsCard({
  channels,
  onSaveChannel,
  onDeleteChannel,
  onClearApiKey,
  onFetchModels,
  onLookupModelsDev,
  writeDisabled = false,
}: ModelSettingsCardProps) {
  const [state, dispatch] = useReducer(
    channelEditorReducer,
    channels,
    createInitialChannelEditorState,
  );
  // Serialize channel saves and keep the latest identity/revision outside React state
  // so rapid model adds cannot race on expected_revision.
  const saveTailRef = useRef(new Map<string, Promise<void>>());
  const savedIdentityRef = useRef(
    new Map<string, { key: string; profileId: number; revision: number }>(),
  );

  useEffect(() => {
    for (const channel of channels) {
      savedIdentityRef.current.set(`id:${channel.profile_id}`, {
        key: `channel-${channel.profile_id}`,
        profileId: channel.profile_id,
        revision: channel.revision,
      });
    }
    dispatch({ type: "sync_channels", channels });
  }, [channels]);

  const activeManagerDraft =
    state.modelManager === null
      ? null
      : (state.drafts.find((draft) => draft.key === state.modelManager?.key) ?? null);

  const findDraft = (key: string) => state.drafts.find((draft) => draft.key === key);

  const chainKeyFor = (draft: ChannelDraft) =>
    draft.profileId !== null ? `id:${draft.profileId}` : `tmp:${draft.key}`;

  const persistDraft = async (draft: ChannelDraft) => {
    const key = draft.key;
    dispatch({ type: "mark_saving", key });
    try {
      const savedChannel = await onSaveChannel(
        draft.profileId,
        toChannelPayload(draft),
        toSelectedModelPayloads(draft),
        draft.revision,
      );
      dispatch({
        type: "mark_saved",
        key,
        channel: savedChannel,
        selectedModels: new Map(draft.selectedModels),
        savedEditVersion: draft.editVersion,
      });
      const identity = {
        key: `channel-${savedChannel.profile_id}`,
        profileId: savedChannel.profile_id,
        revision: savedChannel.revision,
      };
      savedIdentityRef.current.set(`id:${savedChannel.profile_id}`, identity);
      savedIdentityRef.current.set(`tmp:${key}`, identity);
      return savedChannel;
    } catch {
      dispatch({ type: "mark_save_failed", key });
      return null;
    }
  };

  const enqueuePersist = (draft: ChannelDraft) => {
    if (writeDisabled) {
      return;
    }
    const initialChainKey = chainKeyFor(draft);
    const previous = saveTailRef.current.get(initialChainKey) ?? Promise.resolve();
    const next = previous
      .catch(() => undefined)
      .then(async () => {
        const identity =
          savedIdentityRef.current.get(initialChainKey) ??
          (draft.profileId !== null
            ? savedIdentityRef.current.get(`id:${draft.profileId}`)
            : undefined);
        const draftToSave: ChannelDraft = identity
          ? {
              ...draft,
              key: identity.key,
              profileId: identity.profileId,
              revision: identity.revision,
              baseRevision: identity.revision,
              // Saving status would block canSaveChannel; treat queued drafts as dirty.
              status: draft.status === "conflict" ? "conflict" : "dirty",
            }
          : {
              ...draft,
              status: draft.status === "conflict" ? "conflict" : "dirty",
            };
        if (!canSaveChannel(draftToSave)) {
          return;
        }
        await persistDraft(draftToSave);
      });
    saveTailRef.current.set(initialChainKey, next);
    void next.finally(() => {
      if (saveTailRef.current.get(initialChainKey) === next) {
        saveTailRef.current.delete(initialChainKey);
      }
    });
  };

  const autoSaveAfterModelChange = (draft: ChannelDraft) => {
    if (writeDisabled) {
      return;
    }
    if (draft.status === "conflict") {
      toast.error("服务端渠道已更新，请先解决冲突后再保存模型");
      return;
    }
    if (!canSaveChannel(draft)) {
      toast.message("模型已加入草稿，请完善渠道名称与 API 链接后保存");
      return;
    }
    enqueuePersist(draft);
  };

  const handleSave = (key: string) => {
    const draft = findDraft(key);
    if (!draft || !canSaveChannel(draft) || writeDisabled) return;
    enqueuePersist(draft);
  };

  const handleDelete = (key: string) => {
    const draft = findDraft(key);
    if (!draft) return;
    if (draft.profileId === null) {
      dispatch({ type: "remove_draft", key });
      return;
    }
    void (async () => {
      dispatch({ type: "mark_deleting", key });
      try {
        await onDeleteChannel(draft.profileId as number);
        dispatch({ type: "remove_draft", key });
      } catch {
        dispatch({ type: "mark_delete_failed", key });
      }
    })();
  };

  const handleClearApiKey = (key: string) => {
    const draft = findDraft(key);
    if (!draft || draft.profileId === null || draft.revision === null) return;
    void (async () => {
      try {
        await onClearApiKey(draft.profileId as number, draft.revision as number);
        dispatch({ type: "clear_api_key_local", key });
      } catch {
        // The parent mutation reports the error; retain the draft and server revision.
      }
    })();
  };

  const handleFetchModels = (draft: ChannelDraft) => {
    const fingerprint = modelCatalogFingerprint(draft);
    void (async () => {
      dispatch({ type: "fetch_models_start", key: draft.key, fingerprint });
      try {
        const items = await onFetchModels(draft.profileId ?? 0, {
          llm_protocol: draft.protocol,
          llm_base_url: normalizeModelBaseUrl(draft.protocol, draft.baseUrl),
          llm_api_key: draft.apiKey,
          provider_config: draft.providerConfig,
        });
        dispatch({
          type: "fetch_models_success",
          key: draft.key,
          fingerprint,
          items,
        });
      } catch {
        dispatch({ type: "fetch_models_failed", key: draft.key, fingerprint });
      }
    })();
  };

  const totalModels = state.drafts.reduce((sum, draft) => sum + draft.selectedModels.size, 0);

  return (
    <>
      <div className="flex flex-col gap-4">
        {state.drafts.length > 0 ? (
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="text-muted-foreground text-sm">
              共{" "}
              <span className="text-foreground font-semibold tabular-nums">
                {state.drafts.length}
              </span>{" "}
              个渠道，模型池{" "}
              <span className="text-foreground font-semibold tabular-nums">{totalModels}</span>{" "}
              个模型
            </p>
            <Button type="button" onClick={() => dispatch({ type: "add_channel" })}>
              <PlusIcon className="size-4" />
              添加渠道
            </Button>
          </div>
        ) : (
          <Empty className="border-border/60 rounded-xl border border-dashed py-10 md:p-10">
            <EmptyHeader>
              <EmptyMedia variant="icon" className="bg-primary/10 text-primary">
                <NetworkIcon />
              </EmptyMedia>
              <EmptyTitle className="text-base">还没有模型渠道</EmptyTitle>
              <EmptyDescription className="text-sm">
                添加一个渠道接入 OpenAI 兼容或 Claude 模型
                兼容服务，配置密钥并挑选模型后，各阶段即可使用。
              </EmptyDescription>
            </EmptyHeader>
            <EmptyContent>
              <Button type="button" onClick={() => dispatch({ type: "add_channel" })}>
                <PlusIcon className="size-4" />
                添加渠道
              </Button>
            </EmptyContent>
          </Empty>
        )}

        {state.drafts.length > 0 ? (
          <Accordion
            type="multiple"
            value={state.expandedKeys}
            onValueChange={(keys) => dispatch({ type: "set_expanded_keys", keys })}
            className="space-y-3"
          >
            {state.drafts.map((draft, index) => {
              const protocol = protocolMeta(draft.protocol);
              const title = draft.name.trim() || `渠道 ${index + 1}`;
              const savedChannel = channels.find((item) => item.profile_id === draft.profileId);
              const modelCount = draft.selectedModels.size;
              return (
                <div key={draft.key}>
                  <AccordionItem
                    value={draft.key}
                    className={cn(
                      "border-border/60 bg-card/50 overflow-hidden rounded-xl border shadow-xs last:border-b",
                      "hover:border-border hover:shadow-sm",
                      "data-[state=open]:border-border data-[state=open]:shadow-sm",
                    )}
                  >
                    <AccordionTrigger className="hover:bg-muted/30 items-center rounded-none px-4 py-3.5 hover:no-underline sm:px-5 [&>svg]:translate-y-0">
                      <div className="min-w-0 flex-1 text-start">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="truncate text-sm font-semibold tracking-tight">
                            {title}
                          </span>
                          <ChannelStatusBadge draft={draft} />
                        </div>
                        <div className="text-muted-foreground mt-1 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5 text-xs">
                          <span className="truncate">{protocol.label}</span>
                          <MetaDot />
                          {modelCount > 0 ? (
                            <span className="tabular-nums">{modelCount} 个模型</span>
                          ) : (
                            <span className="text-amber-600">尚未添加模型</span>
                          )}
                          {savedChannel ? (
                            <>
                              <MetaDot />
                              <span className="inline-flex items-center gap-1">
                                <KeyRoundIcon className="size-3" />
                                {savedChannel.api_key_configured
                                  ? savedChannel.api_key_last_four
                                    ? `密钥尾号 ${savedChannel.api_key_last_four}`
                                    : "密钥已配置"
                                  : "未配置密钥"}
                              </span>
                            </>
                          ) : null}
                        </div>
                      </div>
                    </AccordionTrigger>
                    <AccordionContent className="border-border/60 border-t px-4 pt-4 pb-4 sm:px-5">
                      <ChannelForm
                        draft={draft}
                        index={index}
                        channels={channels}
                        writeDisabled={writeDisabled}
                        onPatch={(patch, markDirty) =>
                          dispatch({
                            type: "patch_draft",
                            key: draft.key,
                            patch,
                            ...(markDirty === undefined ? {} : { markDirty }),
                          })
                        }
                        onNormalizeBaseUrl={(baseUrl) =>
                          dispatch({
                            type: "normalize_base_url",
                            key: draft.key,
                            baseUrl,
                          })
                        }
                        onEditSelectedModel={(modelKeyValue) =>
                          dispatch({
                            type: "open_model_manager",
                            key: draft.key,
                            modelKey: modelKeyValue,
                          })
                        }
                        onRemoveSelectedModel={(modelKeyValue) =>
                          dispatch({
                            type: "remove_selected_model",
                            key: draft.key,
                            modelKey: modelKeyValue,
                          })
                        }
                        onOpenModelManager={() =>
                          dispatch({ type: "open_model_manager", key: draft.key })
                        }
                        onSave={() => handleSave(draft.key)}
                        onDelete={() => handleDelete(draft.key)}
                        onClearApiKey={() => handleClearApiKey(draft.key)}
                      />
                    </AccordionContent>
                  </AccordionItem>
                </div>
              );
            })}
          </Accordion>
        ) : null}
      </div>

      <ModelPickerDialog
        draft={activeManagerDraft}
        editingModelKey={state.modelManager?.editingModelKey ?? null}
        onOpenChange={(open) => {
          if (!open) {
            dispatch({ type: "close_model_manager" });
          }
        }}
        onFetchModels={() => {
          if (activeManagerDraft) {
            handleFetchModels(activeManagerDraft);
          }
        }}
        onManualNameChange={(value) => {
          if (!activeManagerDraft) {
            return;
          }
          dispatch({
            type: "patch_draft",
            key: activeManagerDraft.key,
            patch: { manualModelName: value },
          });
        }}
        onSelectCatalogModel={(model) => {
          if (!activeManagerDraft) {
            return;
          }
          dispatch({
            type: "patch_draft",
            key: activeManagerDraft.key,
            patch: {
              manualModelName: model.model,
              manualProviderId: model.provider_id ?? "",
            },
          });
        }}
        onManualFieldChange={(field, value) => {
          if (!activeManagerDraft) {
            return;
          }
          dispatch({
            type: "patch_draft",
            key: activeManagerDraft.key,
            patch: { [field]: value },
          });
        }}
        onManualThinkingEffortsChange={(manualThinkingEfforts) => {
          if (!activeManagerDraft) {
            return;
          }
          dispatch({
            type: "patch_draft",
            key: activeManagerDraft.key,
            patch: { manualThinkingEfforts },
          });
        }}
        onLookupModelsDev={async () => {
          if (!activeManagerDraft) return;
          try {
            const model = await onLookupModelsDev(activeManagerDraft.manualModelName);
            dispatch({
              type: "patch_draft",
              key: activeManagerDraft.key,
              patch: {
                manualModelName: model.model_name,
                manualProviderId: model.provider_id,
                manualContextWindowTokens: String(model.context_window_tokens),
                manualMaxOutputTokens: String(model.max_output_tokens),
                manualInputPrice:
                  model.input_price_per_million === null
                    ? ""
                    : String(model.input_price_per_million),
                manualOutputPrice:
                  model.output_price_per_million === null
                    ? ""
                    : String(model.output_price_per_million),
                manualCacheReadPrice:
                  model.cache_read_price_per_million === null
                    ? ""
                    : String(model.cache_read_price_per_million),
                manualCacheWritePrice:
                  model.cache_write_price_per_million === null
                    ? ""
                    : String(model.cache_write_price_per_million),
                manualThinkingEfforts: model.thinking_efforts ?? [],
              },
            });
            toast.success("已从 models.dev 填入模型参数和价格");
          } catch (error) {
            toast.error(error instanceof Error ? error.message : "models.dev 获取失败");
          }
        }}
        onAddManualModel={() => {
          if (!activeManagerDraft || writeDisabled) {
            return;
          }
          if (activeManagerDraft.status === "saving") {
            return;
          }
          const validationMessage = manualModelValidationMessage(activeManagerDraft);
          if (validationMessage) {
            toast.error(validationMessage);
            return;
          }
          const editingModelKey = state.modelManager?.editingModelKey;
          if (editingModelKey) {
            const nextDraft = withUpdatedManualModel(activeManagerDraft, editingModelKey);
            if (nextDraft === null) {
              toast.error("无法使用重复的模型标识覆盖现有模型");
              return;
            }
            dispatch({
              type: "update_manual_model",
              key: activeManagerDraft.key,
              modelKey: editingModelKey,
            });
            autoSaveAfterModelChange(nextDraft);
            dispatch({ type: "close_model_manager" });
            return;
          }
          const result = withManualModel(activeManagerDraft);
          if (result === null) {
            toast.error("该模型已添加到渠道模型池");
            return;
          }
          dispatch({ type: "add_manual_model", key: activeManagerDraft.key });
          if (result.added) {
            autoSaveAfterModelChange(result.draft);
            dispatch({ type: "close_model_manager" });
          }
        }}
      />
    </>
  );
}
