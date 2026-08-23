import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2Icon, CircleDashedIcon, SaveIcon, Trash2Icon } from "lucide-react";
import { toast } from "sonner";

import { QueryErrorState, QueryLoadingState } from "@/components/query-state";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { SecretInput } from "@/components/ui/secret-input";
import { ConfigurationConflictDialog } from "@/features/settings/components/configuration-conflict-dialog";
import { ConfigurationReloadNotice } from "@/features/settings/components/configuration-reload-notice";
import { getSettings, updateSettings } from "@/lib/api";
import { requireRevision } from "@/lib/configuration-revision";
import { getErrorMessage } from "@/lib/format";
import { isApiConflictError, type ApiConflictError } from "@/lib/openapi-client";
import { cn } from "@/lib/utils";

const SETTINGS_QUERY_KEY = ["settings"] as const;

function MxStatusBadge({ configured }: { configured: boolean }) {
  return (
    <Badge
      variant="outline"
      className={cn(
        "h-5 gap-1 rounded-sm px-1.5 text-[11px] leading-none font-medium",
        configured
          ? "border-emerald-500/25 bg-emerald-500/10 text-emerald-700"
          : "border-amber-500/30 bg-amber-500/10 text-amber-700",
      )}
    >
      {configured ? (
        <CheckCircle2Icon className="size-3" />
      ) : (
        <CircleDashedIcon className="size-3" />
      )}
      {configured ? "已配置" : "未配置"}
    </Badge>
  );
}

/** Configure the application-wide MX credential. */
export function MxSettingsPage() {
  const queryClient = useQueryClient();
  const [apiKeyDraft, setApiKeyDraft] = useState("");
  const [conflict, setConflict] = useState<ApiConflictError | null>(null);
  const [needsReload, setNeedsReload] = useState(false);
  const settingsQuery = useQuery({ queryKey: SETTINGS_QUERY_KEY, queryFn: getSettings });
  const settings = settingsQuery.data;

  const reportWriteError = (error: unknown) => {
    if (isApiConflictError(error)) {
      setConflict(error);
      setNeedsReload(true);
      return;
    }
    toast.error(getErrorMessage(error));
  };

  const updateMutation = useMutation({
    mutationFn: updateSettings,
    onSuccess: (updated) => {
      queryClient.setQueryData(SETTINGS_QUERY_KEY, updated);
      setApiKeyDraft("");
      toast.success("妙想设置已保存");
    },
    onError: reportWriteError,
  });

  const reloadServerConfiguration = async () => {
    const result = await settingsQuery.refetch();
    if (result.isError || result.data === undefined) return;
    setApiKeyDraft("");
    setNeedsReload(false);
    setConflict(null);
  };

  if (settingsQuery.isLoading) {
    return <QueryLoadingState label="正在加载妙想设置…" />;
  }
  if (settingsQuery.isError && !settings) {
    return (
      <QueryErrorState
        title="妙想设置加载失败"
        error={settingsQuery.error}
        onRetry={() => void settingsQuery.refetch()}
      />
    );
  }
  if (!settings) return null;

  const disabled = updateMutation.isPending || needsReload;
  const saveApiKey = () => {
    const apiKey = apiKeyDraft.trim();
    if (!apiKey) return;
    updateMutation.mutate({
      expected_revision: requireRevision(settings.revision, "妙想设置"),
      mx_api_key: apiKey,
    });
  };
  const clearApiKey = () => {
    updateMutation.mutate({
      expected_revision: requireRevision(settings.revision, "妙想设置"),
      mx_api_key: null,
    });
  };

  return (
    <section className="w-full max-w-[986px]" aria-label="妙想设置内容">
      <div className="space-y-4">
        {settingsQuery.error ? (
          <p className="text-destructive text-sm">
            后台刷新失败：{getErrorMessage(settingsQuery.error)}
          </p>
        ) : null}
        <ConfigurationReloadNotice visible={needsReload} onReload={reloadServerConfiguration} />
        <form
          className="space-y-3"
          onSubmit={(event) => {
            event.preventDefault();
            saveApiKey();
          }}
        >
          <div className="flex flex-wrap items-center gap-2">
            <label htmlFor="mx-settings-api-key" className="text-sm font-medium">
              妙想 API 密钥
            </label>
            <MxStatusBadge configured={settings.mx.api_key_configured} />
            {settings.mx.api_key_configured && settings.mx.api_key_last_four ? (
              <span className="text-muted-foreground text-xs">
                尾号 {settings.mx.api_key_last_four}
              </span>
            ) : null}
          </div>
          <div className="flex w-[986px] max-w-full flex-col gap-2 md:flex-row">
            <div className="min-w-0 md:min-w-[28rem] md:flex-1">
              <SecretInput
                id="mx-settings-api-key"
                className="w-full"
                value={apiKeyDraft}
                disabled={disabled}
                onChange={(event) => setApiKeyDraft(event.target.value)}
                placeholder={settings.mx.api_key_configured ? "留空保持当前密钥" : "输入 API 密钥"}
                autoComplete="off"
              />
            </div>
            {settings.mx.api_key_configured ? (
              <AlertDialog>
                <AlertDialogTrigger asChild>
                  <Button type="button" variant="outline" disabled={disabled} className="shrink-0">
                    <Trash2Icon className="size-4" />
                    清除
                  </Button>
                </AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle>清除妙想 API 密钥</AlertDialogTitle>
                    <AlertDialogDescription>
                      清除后，所有妙想工具调用将无法执行。
                    </AlertDialogDescription>
                  </AlertDialogHeader>
                  <AlertDialogFooter>
                    <AlertDialogCancel>取消</AlertDialogCancel>
                    <AlertDialogAction disabled={updateMutation.isPending} onClick={clearApiKey}>
                      清除密钥
                    </AlertDialogAction>
                  </AlertDialogFooter>
                </AlertDialogContent>
              </AlertDialog>
            ) : null}
            <Button type="submit" disabled={disabled || !apiKeyDraft.trim()} className="shrink-0">
              <SaveIcon className="size-4" />
              保存密钥
            </Button>
          </div>
        </form>
      </div>

      <ConfigurationConflictDialog
        conflict={conflict}
        onKeepLocal={() => setConflict(null)}
        onReload={reloadServerConfiguration}
      />
    </section>
  );
}
