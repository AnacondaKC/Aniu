import { BoxesIcon, PencilIcon, PlugZapIcon, PlusIcon, Trash2Icon, XIcon } from "lucide-react";

import { normalizeModelBaseUrl } from "@/lib/model-endpoint";
import { thinkingEffortLabel } from "@/lib/thinking-effort";
import type { ModelProfile } from "@/lib/api-types";
import { Button } from "@/components/ui/button";
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
import { Field, FieldContent, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { SecretInput } from "@/components/ui/secret-input";
import { Spinner } from "@/components/ui/spinner";
import { SectionLabel } from "@/features/settings/components/section-label";

import type { ModelProtocol } from "@/lib/api-types";
import {
  PROTOCOL_OPTIONS,
  apiKeyPlaceholder,
  canSaveChannel,
  protocolMeta,
  type ChannelDraft,
  type SelectedModelDraft,
} from "./channel-draft";
import { ProtocolBrandIcon } from "./protocol-brand";

function formatTokenLimit(value: number | null | undefined) {
  if (value == null) return null;
  return value >= 1_000_000
    ? `${(value / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}M`
    : `${(value / 1_000).toLocaleString(undefined, { maximumFractionDigits: 1 })}K`;
}

function modelMetadataSummary(item: SelectedModelDraft) {
  const parts: string[] = [];
  const context = formatTokenLimit(item.context_window_tokens);
  const output = formatTokenLimit(item.max_output_tokens);
  if (context) parts.push(`上下文 ${context}`);
  if (output) parts.push(`输出 ${output}`);
  if (item.input_price_per_million != null || item.output_price_per_million != null) {
    parts.push(
      `输入/输出 $${item.input_price_per_million ?? "-"}/$${item.output_price_per_million ?? "-"}`,
    );
  }
  if (item.thinking_efforts?.length) {
    parts.push(`思考 ${item.thinking_efforts.map(thinkingEffortLabel).join("/")}`);
  }
  return parts.join(" · ");
}

type ChannelFormProps = {
  draft: ChannelDraft;
  index: number;
  channels: ModelProfile[];
  writeDisabled: boolean;
  onPatch: (
    patch: Partial<Pick<ChannelDraft, "name" | "protocol" | "baseUrl" | "apiKey">>,
    markDirty?: boolean,
  ) => void;
  onNormalizeBaseUrl: (baseUrl: string) => void;
  onEditSelectedModel: (modelKey: string) => void;
  onRemoveSelectedModel: (modelKey: string) => void;
  onOpenModelManager: () => void;
  onSave: () => void;
  onDelete: () => void;
  onClearApiKey: () => void;
};

export function ChannelForm({
  draft,
  index,
  channels,
  writeDisabled,
  onPatch,
  onNormalizeBaseUrl,
  onEditSelectedModel,
  onRemoveSelectedModel,
  onOpenModelManager,
  onSave,
  onDelete,
  onClearApiKey,
}: ChannelFormProps) {
  const selectedProtocol = protocolMeta(draft.protocol);
  const title = draft.name.trim() || `渠道 ${index + 1}`;
  const fieldIdPrefix = `channel-${draft.profileId ?? `new-${index}`}`;
  const savedChannel = channels.find((item) => item.profile_id === draft.profileId);
  const deleting = draft.status === "deleting";
  const saving = draft.status === "saving";

  return (
    <div className="flex flex-col gap-5">
      {draft.status === "conflict" ? (
        <p className="rounded-lg border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-300">
          服务端渠道已更新。请先重新加载服务端版本，或在解决冲突前保留本地草稿（不可用旧 revision
          直接保存）。
        </p>
      ) : null}

      <section className="flex flex-col gap-3" aria-label={`渠道 ${title} 连接配置`}>
        <SectionLabel icon={<PlugZapIcon className="size-3.5" />}>连接配置</SectionLabel>

        <div className="grid gap-4 xl:grid-cols-2">
          <Field>
            <FieldLabel htmlFor={`${fieldIdPrefix}-name`}>渠道名称</FieldLabel>
            <FieldContent>
              <Input
                id={`${fieldIdPrefix}-name`}
                value={draft.name}
                onChange={(event) => onPatch({ name: event.target.value }, true)}
                placeholder="例如：OpenAI 主渠道"
              />
            </FieldContent>
          </Field>

          <Field>
            <FieldLabel htmlFor={`${fieldIdPrefix}-protocol`}>协议类型</FieldLabel>
            <FieldContent>
              <Select
                value={draft.protocol}
                onValueChange={(value) => onPatch({ protocol: value as ModelProtocol }, true)}
              >
                <SelectTrigger id={`${fieldIdPrefix}-protocol`} className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {PROTOCOL_OPTIONS.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        <ProtocolBrandIcon protocol={option.value} size="sm" />
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
            </FieldContent>
          </Field>

          <Field>
            <FieldLabel htmlFor={`${fieldIdPrefix}-base-url`}>API 链接</FieldLabel>
            <FieldContent>
              <Input
                id={`${fieldIdPrefix}-base-url`}
                value={draft.baseUrl}
                onChange={(event) => onPatch({ baseUrl: event.target.value }, true)}
                onBlur={() => {
                  onNormalizeBaseUrl(normalizeModelBaseUrl(draft.protocol, draft.baseUrl));
                }}
                placeholder={selectedProtocol.placeholder}
                autoComplete="url"
              />
            </FieldContent>
          </Field>

          <Field>
            <FieldLabel htmlFor={`${fieldIdPrefix}-api-key`}>API 密钥</FieldLabel>
            <FieldContent>
              <div className="flex w-full gap-2">
                <div className="min-w-0 flex-1">
                  <SecretInput
                    id={`${fieldIdPrefix}-api-key`}
                    value={draft.apiKey}
                    onChange={(event) => onPatch({ apiKey: event.target.value }, true)}
                    placeholder={apiKeyPlaceholder(draft, channels)}
                    autoComplete="off"
                  />
                </div>
                {savedChannel?.api_key_configured ? (
                  <Button
                    type="button"
                    variant="outline"
                    disabled={writeDisabled}
                    onClick={onClearApiKey}
                  >
                    清除密钥
                  </Button>
                ) : null}
              </div>
            </FieldContent>
          </Field>
        </div>
      </section>

      <section className="flex flex-col gap-3" aria-label={`渠道 ${title} 模型池`}>
        <SectionLabel icon={<BoxesIcon className="size-3.5" />}>
          模型池
          {draft.selectedModels.size > 0 ? (
            <span className="text-muted-foreground/80 font-normal tabular-nums">
              {draft.selectedModels.size} 个
            </span>
          ) : null}
        </SectionLabel>

        <div className="flex flex-wrap items-center gap-2">
          {[...draft.selectedModels.values()].map((item) => {
            const key = item.provider_id ?? item.model;
            const metadata = modelMetadataSummary(item);
            return (
              <span
                key={key}
                title={metadata || undefined}
                className="group/chip border-border/60 bg-muted/40 hover:border-border hover:bg-muted/70 inline-flex min-h-7 max-w-full items-center gap-1 rounded-lg border py-1 ps-2.5 pe-1 text-xs"
              >
                <span className="flex max-w-[360px] min-w-0 flex-col leading-tight">
                  <span className="text-foreground/90 truncate font-medium">{item.model}</span>
                  {metadata ? (
                    <span className="text-muted-foreground mt-0.5 truncate text-[10px]">
                      {metadata}
                    </span>
                  ) : null}
                </span>
                <Button
                  type="button"
                  size="icon"
                  variant="ghost"
                  className="text-muted-foreground/70 hover:bg-primary/10 hover:text-primary size-5 shrink-0 rounded-md"
                  onClick={() => onEditSelectedModel(key)}
                  aria-label={`修改已添加模型 ${item.model}`}
                >
                  <PencilIcon className="size-3" />
                </Button>
                <Button
                  type="button"
                  size="icon"
                  variant="ghost"
                  className="text-muted-foreground/70 hover:bg-destructive/10 hover:text-destructive size-5 shrink-0 rounded-md"
                  onClick={() => onRemoveSelectedModel(key)}
                  aria-label={`删除已添加模型 ${item.model}`}
                >
                  <XIcon className="size-3" />
                </Button>
              </span>
            );
          })}
          <button
            type="button"
            onClick={onOpenModelManager}
            className="border-border text-muted-foreground hover:border-primary/50 hover:bg-primary/5 hover:text-primary focus-visible:ring-ring/50 focus-visible:border-ring inline-flex h-7 items-center gap-1 rounded-lg border border-dashed px-2.5 text-xs font-medium outline-none focus-visible:ring-[3px]"
          >
            <PlusIcon className="size-3.5" />
            添加模型
          </button>
        </div>
        {draft.selectedModels.size === 0 ? (
          <p className="text-muted-foreground text-xs">
            当前渠道还没有模型：可从渠道拉取可用列表，或手动录入模型名称。
          </p>
        ) : null}
      </section>

      <div className="border-border/60 flex flex-wrap items-center justify-between gap-2 border-t pt-4">
        <AlertDialog>
          <AlertDialogTrigger asChild>
            <Button
              type="button"
              variant="ghost"
              className="text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
              disabled={writeDisabled || deleting}
            >
              {deleting ? <Spinner className="size-4" /> : <Trash2Icon className="size-4" />}
              删除渠道
            </Button>
          </AlertDialogTrigger>
          <AlertDialogContent size="sm">
            <AlertDialogHeader>
              <AlertDialogTitle>删除模型渠道「{title}」</AlertDialogTitle>
              <AlertDialogDescription>
                将永久删除该渠道、其已保存的模型和密钥配置，且无法恢复。
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>取消</AlertDialogCancel>
              <AlertDialogAction variant="destructive" onClick={onDelete}>
                确认删除
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>

        <Button
          type="button"
          disabled={writeDisabled || saving || !canSaveChannel(draft)}
          onClick={onSave}
        >
          {saving ? <Spinner className="size-4" /> : null}
          保存渠道
        </Button>
      </div>
    </div>
  );
}
