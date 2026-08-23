import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import { QueryErrorState, QueryLoadingState } from "@/components/query-state";
import { ConfigurationConflictDialog } from "@/features/settings/components/configuration-conflict-dialog";
import { ConfigurationReloadNotice } from "@/features/settings/components/configuration-reload-notice";
import { ModelSettingsCard } from "@/features/settings/model-channels";
import {
  clearModelChannelApiKey,
  createModelChannel,
  deleteModelChannel,
  fetchModelCatalog,
  listModelChannels,
  lookupModelsDevModel,
  updateModelChannel,
} from "@/lib/api";
import type {
  ModelCatalogFetchPayload,
  ModelProfilePayload,
  SelectedModelPayload,
} from "@/lib/api-types";
import { requireRevision } from "@/lib/configuration-revision";
import { getErrorMessage } from "@/lib/format";
import { isApiConflictError, type ApiConflictError } from "@/lib/openapi-client";

const MODEL_CHANNELS_QUERY_KEY = ["modelChannels"] as const;

/** Manage model providers and the model pool available to every pipeline stage. */
export function ModelChannelsSettingsPage() {
  const queryClient = useQueryClient();
  const [conflict, setConflict] = useState<ApiConflictError | null>(null);
  const [needsReload, setNeedsReload] = useState(false);
  const [channelReloadKey, setChannelReloadKey] = useState(0);
  const channelsQuery = useQuery({
    queryKey: MODEL_CHANNELS_QUERY_KEY,
    queryFn: listModelChannels,
  });
  const channels = channelsQuery.data ?? [];
  const channelsReady = channelsQuery.isSuccess && channelsQuery.data !== undefined;

  const reportWriteError = (error: unknown) => {
    if (isApiConflictError(error)) {
      setConflict(error);
      setNeedsReload(true);
      return;
    }
    toast.error(getErrorMessage(error));
  };

  const modelChannelDeleteMutation = useMutation({
    mutationFn: ({ channelId, revision }: { channelId: number; revision: number }) =>
      deleteModelChannel(channelId, revision),
    onSuccess: (_, { channelId }) => {
      queryClient.setQueryData<Awaited<ReturnType<typeof listModelChannels>>>(
        MODEL_CHANNELS_QUERY_KEY,
        (current) => (current ?? []).filter((item) => item.profile_id !== channelId),
      );
      toast.success("模型渠道已删除");
    },
    onError: reportWriteError,
  });
  const modelCatalogMutation = useMutation({
    mutationFn: ({
      channelId,
      payload,
    }: {
      channelId: number;
      payload: ModelCatalogFetchPayload;
    }) => fetchModelCatalog(channelId, payload),
    onError: (error) => toast.error(getErrorMessage(error)),
  });

  const refreshChannels = () => channelsQuery.refetch();
  const reloadServerConfiguration = async () => {
    const result = await refreshChannels();
    if (result.isError || result.data === undefined) return;
    setNeedsReload(false);
    setChannelReloadKey((value) => value + 1);
    setConflict(null);
  };

  const saveModelChannel = async (
    channelId: number | null,
    channelPayload: ModelProfilePayload,
    selectedModelsPayload: SelectedModelPayload[],
    expectedRevision: number | null,
  ) => {
    try {
      if (needsReload) {
        throw new Error("请先重新加载服务端版本，再保存模型渠道");
      }
      const savedChannel =
        channelId === null
          ? await createModelChannel({
              ...channelPayload,
              selected_models: selectedModelsPayload,
            })
          : await updateModelChannel(channelId, {
              ...channelPayload,
              selected_models: selectedModelsPayload,
              expected_revision: requireRevision(expectedRevision, "模型渠道"),
            });
      queryClient.setQueryData<Awaited<ReturnType<typeof listModelChannels>>>(
        MODEL_CHANNELS_QUERY_KEY,
        (current) => {
          const existing = current ?? [];
          const index = existing.findIndex((item) => item.profile_id === savedChannel.profile_id);
          if (index === -1) return [...existing, savedChannel];
          return existing.map((item) =>
            item.profile_id === savedChannel.profile_id ? savedChannel : item,
          );
        },
      );
      toast.success(channelId === null ? "模型渠道已创建" : "模型渠道已保存");
      return savedChannel;
    } catch (error) {
      reportWriteError(error);
      throw error;
    }
  };

  if (channelsQuery.isLoading) {
    return <QueryLoadingState label="正在加载模型渠道…" />;
  }
  if (channelsQuery.isError && !channelsQuery.data) {
    return (
      <QueryErrorState
        title="模型渠道加载失败"
        error={channelsQuery.error}
        onRetry={() => void refreshChannels()}
      />
    );
  }

  return (
    <div className="flex flex-col gap-6">
      {channelsQuery.error ? (
        <div
          role="alert"
          className="rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-800 dark:text-amber-200"
        >
          正在显示上次成功加载的模型渠道；后台刷新失败：{getErrorMessage(channelsQuery.error)}
        </div>
      ) : null}
      <ModelSettingsCard
        key={channelReloadKey}
        channels={channels}
        writeDisabled={needsReload || !channelsReady}
        onSaveChannel={saveModelChannel}
        onDeleteChannel={(channelId) => {
          const channel = channels.find((item) => item.profile_id === channelId);
          if (!channel) return Promise.reject(new Error("模型渠道不存在或已被删除"));
          return modelChannelDeleteMutation.mutateAsync({
            channelId,
            revision: channel.revision,
          });
        }}
        onClearApiKey={async (channelId, revision) => {
          try {
            const result = await clearModelChannelApiKey(channelId, revision);
            queryClient.setQueryData<Awaited<ReturnType<typeof listModelChannels>>>(
              MODEL_CHANNELS_QUERY_KEY,
              (current) =>
                (current ?? []).map((channel) =>
                  channel.profile_id === channelId ? result : channel,
                ),
            );
            toast.success("模型渠道 API 密钥已清除");
            return result;
          } catch (error) {
            reportWriteError(error);
            throw error;
          }
        }}
        onFetchModels={(channelId, payload) =>
          modelCatalogMutation.mutateAsync({ channelId, payload })
        }
        onLookupModelsDev={lookupModelsDevModel}
      />
      <ConfigurationReloadNotice
        visible={needsReload && conflict === null}
        onReload={reloadServerConfiguration}
      />
      <ConfigurationConflictDialog
        conflict={conflict}
        onKeepLocal={() => setConflict(null)}
        onReload={reloadServerConfiguration}
      />
    </div>
  );
}
