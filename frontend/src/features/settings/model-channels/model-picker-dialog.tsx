import { useEffect, useRef, useState } from "react";
import { CloudDownloadIcon, PlusIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";

import {
  hasChannelEndpoint,
  manualModelValidationMessage,
  protocolMeta,
  type ChannelDraft,
} from "./channel-draft";
import type { ModelCatalogItem } from "@/lib/api-types";
import { THINKING_EFFORT_OPTIONS, type ThinkingEffort } from "@/lib/thinking-effort";
import { ProtocolBrandIcon } from "./protocol-brand";

type ModelPickerDialogProps = {
  draft: ChannelDraft | null;
  editingModelKey: string | null;
  onOpenChange: (open: boolean) => void;
  onFetchModels: () => void;
  onManualNameChange: (value: string) => void;
  onSelectCatalogModel: (model: ModelCatalogItem) => void;
  onManualFieldChange: (
    field:
      | "manualContextWindowTokens"
      | "manualMaxOutputTokens"
      | "manualInputPrice"
      | "manualOutputPrice"
      | "manualCacheReadPrice"
      | "manualCacheWritePrice",
    value: string,
  ) => void;
  onManualThinkingEffortsChange: (values: ThinkingEffort[]) => void;
  onLookupModelsDev: () => Promise<void>;
  onAddManualModel: () => void;
};

export function ModelPickerDialog({
  draft: externalDraft,
  editingModelKey,
  onOpenChange,
  onFetchModels,
  onManualNameChange,
  onSelectCatalogModel,
  onManualFieldChange,
  onManualThinkingEffortsChange,
  onLookupModelsDev,
  onAddManualModel,
}: ModelPickerDialogProps) {
  const draft = externalDraft;
  const [modelsDevPending, setModelsDevPending] = useState(false);
  const fetchRequestedForKeyRef = useRef<string | null>(null);
  const wasFetchPendingRef = useRef(false);
  const fetchPending = draft?.fetchPending ?? false;
  const hasFetchedModels = draft?.hasFetchedModels ?? false;

  useEffect(() => {
    if (draft === null || hasFetchedModels || (wasFetchPendingRef.current && !fetchPending)) {
      fetchRequestedForKeyRef.current = null;
    }
    wasFetchPendingRef.current = fetchPending;
  }, [draft, fetchPending, hasFetchedModels]);

  const fillFromModelsDev = async () => {
    if (!draft?.manualModelName.trim() || modelsDevPending) return;
    setModelsDevPending(true);
    try {
      await onLookupModelsDev();
    } finally {
      setModelsDevPending(false);
    }
  };

  const openModelList = () => {
    if (
      draft &&
      fetchRequestedForKeyRef.current !== draft.key &&
      !draft.hasFetchedModels &&
      !draft.fetchPending &&
      hasChannelEndpoint(draft)
    ) {
      fetchRequestedForKeyRef.current = draft.key;
      onFetchModels();
    }
  };

  const validationMessage =
    draft && draft.manualModelName.trim() ? manualModelValidationMessage(draft) : null;

  return (
    <Dialog open={externalDraft !== null} onOpenChange={onOpenChange}>
      <DialogContent
        className="flex max-h-[85vh] flex-col gap-0 overflow-hidden p-0 sm:max-w-3xl"
        showCloseButton
      >
        {draft ? (
          <>
            <DialogHeader className="border-b px-6 py-5">
              <div className="flex items-center gap-3">
                <ProtocolBrandIcon protocol={draft.protocol} />
                <div className="min-w-0 flex-1 space-y-1 text-start">
                  <DialogTitle>{editingModelKey ? "修改模型" : "添加模型"}</DialogTitle>
                  <DialogDescription className="truncate">
                    {draft.name.trim() || "未命名渠道"} · {protocolMeta(draft.protocol).label}
                  </DialogDescription>
                </div>
              </div>
            </DialogHeader>

            <div className="min-h-0 flex-1 overflow-y-auto px-6 py-5">
              <div className="space-y-4">
                <p className="text-muted-foreground text-sm">
                  点击下拉框可从当前渠道选择，或直接输入名称后从 models.dev 一键填入。
                </p>
                <div className="flex flex-col gap-4">
                  <div className="flex flex-col gap-2 sm:flex-row">
                    <div className="min-w-0 flex-1">
                      <Input
                        id="manual-model-name"
                        aria-label="模型名称"
                        value={draft.manualModelName}
                        onChange={(event) => onManualNameChange(event.target.value)}
                        onKeyDown={(event) => {
                          if (event.key === "Enter") {
                            event.preventDefault();
                            onAddManualModel();
                          }
                        }}
                        placeholder="输入模型名称"
                        disabled={draft.status === "saving"}
                        autoComplete="off"
                      />
                    </div>
                    <Select
                      value={
                        draft.fetchedModels.some((item) => item.model === draft.manualModelName)
                          ? draft.manualModelName
                          : ""
                      }
                      disabled={draft.status === "saving"}
                      onOpenChange={(open) => {
                        if (open) openModelList();
                      }}
                      onValueChange={(value) => {
                        const selectedModel = draft.fetchedModels.find(
                          (item) => item.model === value,
                        );
                        if (selectedModel) onSelectCatalogModel(selectedModel);
                      }}
                    >
                      <SelectTrigger aria-label="从渠道选择模型" className="w-full sm:w-64">
                        <SelectValue
                          placeholder={draft.fetchPending ? "正在获取模型列表…" : "从渠道选择模型"}
                        />
                      </SelectTrigger>
                      <SelectContent>
                        {draft.fetchPending ? (
                          <div className="text-muted-foreground flex items-center gap-2 px-3 py-2 text-sm">
                            <Spinner className="size-4" />
                            正在获取模型列表…
                          </div>
                        ) : !hasChannelEndpoint(draft) ? (
                          <div className="text-muted-foreground px-3 py-2 text-sm">
                            请先填写渠道 API 链接
                          </div>
                        ) : !draft.hasFetchedModels || draft.fetchedModels.length === 0 ? (
                          <div className="text-muted-foreground px-3 py-2 text-sm">
                            当前渠道没有可选模型
                          </div>
                        ) : (
                          draft.fetchedModels.map((item) => (
                            <SelectItem key={item.provider_id ?? item.model} value={item.model}>
                              {item.model}
                            </SelectItem>
                          ))
                        )}
                      </SelectContent>
                    </Select>
                    <Button
                      type="button"
                      variant="outline"
                      className="shrink-0"
                      disabled={
                        draft.manualModelName.trim().length === 0 ||
                        draft.status === "saving" ||
                        modelsDevPending
                      }
                      onClick={() => void fillFromModelsDev()}
                    >
                      {modelsDevPending ? <Spinner className="size-4" /> : <CloudDownloadIcon />}
                      models.dev 一键填入
                    </Button>
                  </div>

                  <p className="text-muted-foreground text-xs font-medium">模型限制</p>
                  <div className="grid gap-3 sm:grid-cols-2">
                    <label className="text-muted-foreground flex flex-col gap-1.5 text-xs font-medium">
                      最大上下文 Token
                      <Input
                        aria-label="最大上下文 Token"
                        type="number"
                        min={1000}
                        value={draft.manualContextWindowTokens}
                        onChange={(event) =>
                          onManualFieldChange("manualContextWindowTokens", event.target.value)
                        }
                        placeholder="例如 128000"
                      />
                    </label>
                    <label className="text-muted-foreground flex flex-col gap-1.5 text-xs font-medium">
                      最大输出 Token
                      <Input
                        aria-label="最大输出 Token"
                        type="number"
                        min={1}
                        value={draft.manualMaxOutputTokens}
                        onChange={(event) =>
                          onManualFieldChange("manualMaxOutputTokens", event.target.value)
                        }
                        placeholder="例如 32768"
                      />
                    </label>
                  </div>

                  <p className="text-muted-foreground text-xs font-medium">
                    价格（USD / 1M Token）
                  </p>
                  <div className="grid gap-3 sm:grid-cols-2">
                    <label className="text-muted-foreground flex flex-col gap-1.5 text-xs font-medium">
                      输入价格
                      <Input
                        aria-label="输入价格"
                        type="number"
                        min={0}
                        step="any"
                        value={draft.manualInputPrice}
                        onChange={(event) =>
                          onManualFieldChange("manualInputPrice", event.target.value)
                        }
                        placeholder="例如 0.14"
                      />
                    </label>
                    <label className="text-muted-foreground flex flex-col gap-1.5 text-xs font-medium">
                      输出价格
                      <Input
                        aria-label="输出价格"
                        type="number"
                        min={0}
                        step="any"
                        value={draft.manualOutputPrice}
                        onChange={(event) =>
                          onManualFieldChange("manualOutputPrice", event.target.value)
                        }
                        placeholder="例如 0.28"
                      />
                    </label>
                    <label className="text-muted-foreground flex flex-col gap-1.5 text-xs font-medium">
                      缓存读取价格
                      <Input
                        aria-label="缓存读取价格"
                        type="number"
                        min={0}
                        step="any"
                        value={draft.manualCacheReadPrice}
                        onChange={(event) =>
                          onManualFieldChange("manualCacheReadPrice", event.target.value)
                        }
                        placeholder="例如 0.0028"
                      />
                    </label>
                    <label className="text-muted-foreground flex flex-col gap-1.5 text-xs font-medium">
                      缓存写入价格
                      <Input
                        aria-label="缓存写入价格"
                        type="number"
                        min={0}
                        step="any"
                        value={draft.manualCacheWritePrice}
                        onChange={(event) =>
                          onManualFieldChange("manualCacheWritePrice", event.target.value)
                        }
                        placeholder="例如 0.014"
                      />
                    </label>
                  </div>

                  <fieldset className="space-y-2">
                    <legend className="text-muted-foreground text-xs font-medium">
                      可用思考强度 <span lang="en">Available Thinking Efforts</span>
                    </legend>
                    <div className="grid gap-2 sm:grid-cols-3">
                      {THINKING_EFFORT_OPTIONS.map((option) => {
                        const checked = draft.manualThinkingEfforts.includes(option.value);
                        return (
                          <label
                            key={option.value}
                            className="border-border/70 hover:bg-muted/50 has-data-[state=checked]:border-primary has-data-[state=checked]:bg-primary/5 flex h-10 cursor-pointer items-center gap-2 rounded-md border px-3 text-sm font-medium transition-colors"
                          >
                            <Checkbox
                              checked={checked}
                              disabled={draft.status === "saving"}
                              aria-label={`启用${option.label}思考强度`}
                              onCheckedChange={(nextChecked) => {
                                const selected = new Set(draft.manualThinkingEfforts);
                                if (nextChecked === true) selected.add(option.value);
                                else selected.delete(option.value);
                                onManualThinkingEffortsChange(
                                  THINKING_EFFORT_OPTIONS.filter((item) =>
                                    selected.has(item.value),
                                  ).map((item) => item.value),
                                );
                              }}
                            />
                            <span className="min-w-0 truncate">
                              {option.label} <span lang="en">{option.englishLabel}</span>
                            </span>
                          </label>
                        );
                      })}
                    </div>
                  </fieldset>

                  <div className="flex justify-end">
                    <div className="flex flex-col items-end gap-1.5">
                      {validationMessage ? (
                        <p className="text-destructive text-xs" role="alert">
                          {validationMessage}
                        </p>
                      ) : null}
                      {draft.status === "conflict" ? (
                        <p className="text-destructive text-xs" role="alert">
                          服务端渠道已更新，请重新加载后再保存修改
                        </p>
                      ) : null}
                      <Button
                        type="button"
                        disabled={
                          !draft.manualModelName.trim() ||
                          draft.status === "saving" ||
                          draft.status === "conflict"
                        }
                        onClick={onAddManualModel}
                      >
                        {draft.status === "saving" ? (
                          <Spinner className="size-4" />
                        ) : (
                          <PlusIcon className="size-4" />
                        )}
                        {editingModelKey ? "保存修改" : "添加并保存"}
                      </Button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </>
        ) : null}
      </DialogContent>
    </Dialog>
  );
}
