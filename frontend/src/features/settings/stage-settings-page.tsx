import { useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  CpuIcon,
  FileTextIcon,
  LayersIcon,
  MoonIcon,
  PlayIcon,
  Save,
  ScrollTextIcon,
  Settings2,
  SlidersHorizontal,
} from "lucide-react";

import { QueryErrorState, QueryLoadingState } from "@/components/query-state";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
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
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Spinner } from "@/components/ui/spinner";
import { Textarea } from "@/components/ui/textarea";
import { SectionLabel } from "@/features/settings/components/section-label";
import {
  downloadConfig,
  listPromptConfigs,
  parseImportedConfig,
  savePromptConfig,
  type PromptProfileConfig,
} from "@/features/settings/prompt-configs";
import { toast } from "sonner";
import { cn } from "@/lib/utils";
import {
  DEFAULT_THINKING_EFFORT_VALUE,
  isThinkingEffort,
  thinkingEffortLabel,
  type ThinkingEffort,
} from "@/lib/thinking-effort";
import { getSettings, listModelChannels, updateSettings } from "@/lib/api";
import type { StageSettings, UpdateSettingsPayload } from "@/lib/api-types";
import { isApiConflictError } from "@/lib/openapi-client";

const STAGE_DEFINITIONS = [
  {
    id: "Run",
    sequence: 1,
    label: "执行阶段",
    icon: PlayIcon,
    shortDescription: "研究、判断并执行交易",
    description: "配置连续执行任务与生成 Markdown 报告的方式。",
  },
  {
    id: "Summary",
    sequence: 2,
    label: "总结阶段",
    icon: FileTextIcon,
    shortDescription: "生成安全展示报告",
    description: "配置基于运行证据生成 HTML 总结的方式。",
  },
  {
    id: "Dream",
    sequence: 3,
    label: "梦境阶段",
    icon: MoonIcon,
    shortDescription: "整理长期记忆",
    description: "配置夜间阅读报告并维护长期记忆的方式。",
  },
] as const;

function stageMetaById(stageId: string) {
  return STAGE_DEFINITIONS.find((stage) => stage.id === stageId);
}

const SETTINGS_QUERY_KEY = ["settings"] as const;
const DEFAULT_DREAM_SCHEDULE_TIME = "00:30";
const DREAM_SCHEDULE_TIME_PATTERN = /^(?:[01]\d|2[0-3]):[0-5]\d$/;
const GLOBAL_TAB_ID = "Global";
/** Reserved select value: the currently effective (server-side) configuration. */
const CURRENT_PROMPT_CONFIG_VALUE = "__current_effective__";
type SettingsTabId = string;

type StageDraft = {
  modelId: string;
  thinkingEffort: string;
  temperature: string;
  topP: string;
  prompt: string;
  dreamScheduleTime: string;
};

type GlobalDraft = {
  globalPrompt: string;
  // value applies the chosen model and thinking preset to all stages on save.
  globalModelId: string;
  globalThinkingEffort: string;
};

function persistedThinkingEffort(value: string): ThinkingEffort | null {
  return isThinkingEffort(value) ? value : null;
}

function stageThinkingEffortValue(stage: StageSettings): string {
  return stage.thinking_effort ?? DEFAULT_THINKING_EFFORT_VALUE;
}

function commonStageThinkingEffort(settings: Awaited<ReturnType<typeof getSettings>>): string {
  const efforts = new Set(settings.stage_settings.map(stageThinkingEffortValue));
  return efforts.size === 1 ? [...efforts][0]! : "";
}

function commonStageModelId(settings: Awaited<ReturnType<typeof getSettings>>): string {
  const ids = new Set(settings.stage_settings.map((stage) => stage.model_selected_model_id ?? 0));
  if (ids.size !== 1) return "";
  const [only] = ids;
  return only ? String(only) : "";
}

function toStageDraft(
  stage: StageSettings,
  dreamScheduleTime = DEFAULT_DREAM_SCHEDULE_TIME,
): StageDraft {
  return {
    modelId: stage.model_selected_model_id ? String(stage.model_selected_model_id) : "",
    thinkingEffort: stageThinkingEffortValue(stage),
    temperature: String(stage.temperature),
    topP: String(stage.top_p),
    prompt: stage.prompt,
    dreamScheduleTime,
  };
}

function toGlobalDraft(settings: Awaited<ReturnType<typeof getSettings>>): GlobalDraft {
  return {
    globalPrompt: settings.prompt_profile.global_prompt,
    globalModelId: commonStageModelId(settings),
    globalThinkingEffort: commonStageThinkingEffort(settings),
  };
}

function sortStages(stages: StageSettings[]) {
  const rank = new Map<string, number>(STAGE_DEFINITIONS.map((stage, index) => [stage.id, index]));
  return [...stages].sort((a, b) => (rank.get(a.stage_id) ?? 99) - (rank.get(b.stage_id) ?? 99));
}

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error && error.message ? error.message : fallback;
}

/** Configure shared rules separately from the settings owned by each pipeline stage. */
export function StageSettingsPage() {
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState<SettingsTabId>(GLOBAL_TAB_ID);
  const [stageDraft, setStageDraft] = useState<StageDraft | null>(null);
  const [globalDraft, setGlobalDraft] = useState<GlobalDraft | null>(null);
  const [reloadRequired, setReloadRequired] = useState(false);
  const [localPromptConfigs, setLocalPromptConfigs] = useState<PromptProfileConfig[]>(() =>
    listPromptConfigs(),
  );
  const [selectedPromptConfig, setSelectedPromptConfig] = useState<string>(
    CURRENT_PROMPT_CONFIG_VALUE,
  );
  const [saveConfigDialogOpen, setSaveConfigDialogOpen] = useState(false);
  const [saveConfigName, setSaveConfigName] = useState("");
  const configFileInputRef = useRef<HTMLInputElement | null>(null);
  const stageDraftGenerationRef = useRef(0);
  const globalDraftGenerationRef = useRef(0);

  const settingsQuery = useQuery({ queryKey: SETTINGS_QUERY_KEY, queryFn: getSettings });
  const channelsQuery = useQuery({ queryKey: ["modelChannels"], queryFn: listModelChannels });

  const stages = useMemo(
    () => sortStages(settingsQuery.data?.stage_settings ?? []),
    [settingsQuery.data?.stage_settings],
  );
  const activeStage =
    activeTab === GLOBAL_TAB_ID ? null : stages.find((item) => item.stage_id === activeTab);
  const activeMeta = activeStage ? stageMetaById(activeStage.stage_id) : undefined;
  const effectiveStageDraft = activeStage
    ? (stageDraft ?? toStageDraft(activeStage, settingsQuery.data?.dream_schedule_time))
    : null;
  const effectiveGlobalDraft = settingsQuery.data
    ? (globalDraft ?? toGlobalDraft(settingsQuery.data))
    : null;

  const configuredChannels = useMemo(
    () =>
      (channelsQuery.data ?? []).filter(
        (channel) => channel.enabled && channel.selected_models.length > 0,
      ),
    [channelsQuery.data],
  );
  const configuredModels = useMemo(
    () =>
      configuredChannels.flatMap((channel) =>
        channel.selected_models.map((model) => ({ channel, model })),
      ),
    [configuredChannels],
  );

  function thinkingEffortsForModel(modelId: string): ThinkingEffort[] {
    const model = configuredModels.find(
      ({ model: item }) => item.selected_model_id === Number(modelId),
    )?.model;
    return (model?.thinking_efforts ?? []).filter(isThinkingEffort);
  }
  const saveStageMutation = useMutation({
    mutationFn: ({ payload }: { payload: UpdateSettingsPayload; draftGeneration: number }) =>
      updateSettings(payload),
    onSuccess: (updated, submission) => {
      queryClient.setQueryData(SETTINGS_QUERY_KEY, updated);
      if (stageDraftGenerationRef.current === submission.draftGeneration) {
        setStageDraft(null);
      }
      toast.success("阶段设置已保存", { description: "后续运行将使用新的阶段配置。" });
    },
    onError: (error) => {
      if (isApiConflictError(error)) {
        setReloadRequired(true);
        toast.error("配置已被其他会话更新", { description: "请重新加载后再保存。" });
        return;
      }
      toast.error("保存阶段设置失败", { description: errorMessage(error, "请稍后重试。") });
    },
  });

  const saveGlobalMutation = useMutation({
    mutationFn: ({
      payload,
    }: {
      payload: UpdateSettingsPayload;
      globalDraftGeneration: number;
      stageDraftGeneration: number;
    }) => updateSettings(payload),
    onSuccess: (updated, submission) => {
      queryClient.setQueryData(SETTINGS_QUERY_KEY, updated);
      if (globalDraftGenerationRef.current === submission.globalDraftGeneration) {
        setGlobalDraft(null);
      }
      if (stageDraftGenerationRef.current === submission.stageDraftGeneration) {
        setStageDraft(null);
      }
      toast.success("全局设置已保存", { description: "通用规则已同步到所有阶段。" });
    },
    onError: (error) => {
      if (isApiConflictError(error)) {
        setReloadRequired(true);
        toast.error("配置已被其他会话更新", { description: "请重新加载后再保存。" });
        return;
      }
      toast.error("保存全局设置失败", { description: errorMessage(error, "请稍后重试。") });
    },
  });

  function updateDraft(patch: Partial<StageDraft>) {
    if (!effectiveStageDraft) return;
    stageDraftGenerationRef.current += 1;
    setStageDraft({ ...effectiveStageDraft, ...patch });
  }

  function updateGlobalDraft(patch: Partial<GlobalDraft>) {
    if (!effectiveGlobalDraft) return;
    globalDraftGenerationRef.current += 1;
    setGlobalDraft({ ...effectiveGlobalDraft, ...patch });
  }

  function selectStageModel(modelId: string) {
    if (!effectiveStageDraft) return;
    const supportedEfforts = thinkingEffortsForModel(modelId);
    const currentEffort = persistedThinkingEffort(effectiveStageDraft.thinkingEffort);
    updateDraft({
      modelId,
      thinkingEffort:
        currentEffort && supportedEfforts.includes(currentEffort)
          ? currentEffort
          : DEFAULT_THINKING_EFFORT_VALUE,
    });
  }

  function selectGlobalModel(modelId: string) {
    if (!effectiveGlobalDraft) return;
    const supportedEfforts = thinkingEffortsForModel(modelId);
    const currentEffort = persistedThinkingEffort(effectiveGlobalDraft.globalThinkingEffort);
    updateGlobalDraft({
      globalModelId: modelId,
      globalThinkingEffort:
        currentEffort && supportedEfforts.includes(currentEffort)
          ? currentEffort
          : DEFAULT_THINKING_EFFORT_VALUE,
    });
  }

  function selectStage(stageId: string) {
    stageDraftGenerationRef.current += 1;
    setActiveTab(stageId);
    setStageDraft(null);
  }

  function selectGlobal() {
    globalDraftGenerationRef.current += 1;
    setActiveTab(GLOBAL_TAB_ID);
    setGlobalDraft(null);
  }

  async function reloadSettings() {
    const result = await settingsQuery.refetch();
    if (result.isError || result.data === undefined) return;
    stageDraftGenerationRef.current += 1;
    globalDraftGenerationRef.current += 1;
    setReloadRequired(false);
    setStageDraft(null);
    setGlobalDraft(null);
  }

  function saveStage() {
    if (!settingsQuery.data || !activeStage || !effectiveStageDraft || reloadRequired) return;
    const modelId = Number(effectiveStageDraft.modelId);
    const thinkingEffort = persistedThinkingEffort(effectiveStageDraft.thinkingEffort);
    const temperature = Number(effectiveStageDraft.temperature);
    const topP = Number(effectiveStageDraft.topP);
    if (!Number.isInteger(modelId) || modelId <= 0) {
      toast.error("请选择模型");
      return;
    }
    if (
      thinkingEffort !== null &&
      !thinkingEffortsForModel(effectiveStageDraft.modelId).includes(thinkingEffort)
    ) {
      toast.error("所选模型不支持该思考强度");
      return;
    }
    if (!Number.isFinite(temperature) || temperature < 0 || temperature > 2) {
      toast.error("Temperature 必须介于 0 和 2 之间");
      return;
    }
    if (!Number.isFinite(topP) || topP < 0 || topP > 1) {
      toast.error("Top P 必须介于 0 和 1 之间");
      return;
    }
    if (!effectiveStageDraft.prompt.trim()) {
      toast.error("请填写阶段提示词");
      return;
    }
    const dreamScheduleTime = effectiveStageDraft.dreamScheduleTime.trim();
    if (activeStage.stage_id === "Dream" && !DREAM_SCHEDULE_TIME_PATTERN.test(dreamScheduleTime)) {
      toast.error("运行时间必须使用 HH:MM 格式");
      return;
    }
    const updatedStage: StageSettings = {
      stage_id: activeStage.stage_id,
      model_selected_model_id: modelId,
      thinking_effort: thinkingEffort,
      temperature,
      top_p: topP,
      prompt: effectiveStageDraft.prompt.trim(),
    };
    const stageSettings = stages.map((stage) =>
      stage.stage_id === updatedStage.stage_id ? updatedStage : stage,
    );
    saveStageMutation.mutate({
      payload: {
        expected_revision: settingsQuery.data.revision,
        stage_settings: stageSettings,
        ...(activeStage.stage_id === "Dream" ? { dream_schedule_time: dreamScheduleTime } : {}),
        prompt_profile: {
          ...settingsQuery.data.prompt_profile,
          ...(activeStage.stage_id === "Run"
            ? { run_prompt: updatedStage.prompt }
            : activeStage.stage_id === "Summary"
              ? { summary_prompt: updatedStage.prompt }
              : { dream_prompt: updatedStage.prompt }),
        },
      },
      draftGeneration: stageDraftGenerationRef.current,
    });
  }

  function saveGlobal() {
    if (!settingsQuery.data || !effectiveGlobalDraft || reloadRequired) return;
    const chosenModelId = effectiveGlobalDraft.globalModelId;
    const thinkingEffort = persistedThinkingEffort(effectiveGlobalDraft.globalThinkingEffort);
    if (
      chosenModelId !== "" &&
      thinkingEffort !== null &&
      !thinkingEffortsForModel(chosenModelId).includes(thinkingEffort)
    ) {
      toast.error("所选模型不支持该思考强度");
      return;
    }
    const syncModelAndThinking =
      chosenModelId !== "" &&
      (chosenModelId !== commonStageModelId(settingsQuery.data) ||
        effectiveGlobalDraft.globalThinkingEffort !==
          commonStageThinkingEffort(settingsQuery.data));
    saveGlobalMutation.mutate({
      payload: {
        expected_revision: settingsQuery.data.revision,
        prompt_profile: {
          ...settingsQuery.data.prompt_profile,
          global_prompt: effectiveGlobalDraft.globalPrompt.trim(),
        },
        // The server synchronizes every persisted stage map, including stages
        // whose model is not configured yet.
        ...(syncModelAndThinking
          ? {
              stage_settings: stages.map((stage) => ({
                ...stage,
                model_selected_model_id: Number(chosenModelId),
                thinking_effort: thinkingEffort,
              })),
            }
          : {}),
      },
      globalDraftGeneration: globalDraftGenerationRef.current,
      stageDraftGeneration: stageDraftGenerationRef.current,
    });
  }

  const applyPromptConfigMutation = useMutation({
    mutationFn: (config: PromptProfileConfig) => {
      const settings = settingsQuery.data!;
      const dreamPrompt = config.dream_prompt.trim() || settings.prompt_profile.dream_prompt;
      return updateSettings({
        expected_revision: settings.revision,
        prompt_profile: {
          schema: settings.prompt_profile.schema,
          name: config.name,
          description: settings.prompt_profile.description,
          global_prompt: config.global_prompt,
          run_prompt: config.run_prompt,
          summary_prompt: config.summary_prompt,
          dream_prompt: dreamPrompt,
        },
        stage_settings: stages.map((stage) => ({
          ...stage,
          prompt:
            stage.stage_id === "Run"
              ? config.run_prompt
              : stage.stage_id === "Summary"
                ? config.summary_prompt
                : dreamPrompt,
        })),
      });
    },
    onSuccess: (updated) => {
      queryClient.setQueryData(SETTINGS_QUERY_KEY, updated);
      setGlobalDraft(null);
      setStageDraft(null);
      toast.success("提示词配置已应用", { description: "全局与两个阶段的提示词已切换。" });
    },
    onError: (error) => {
      if (isApiConflictError(error)) {
        setReloadRequired(true);
        toast.error("配置已被其他会话更新", { description: "请重新加载后再切换。" });
        return;
      }
      toast.error("应用提示词配置失败", { description: errorMessage(error, "请稍后重试。") });
    },
  });

  function buildCurrentPromptConfig(): PromptProfileConfig | null {
    const settings = settingsQuery.data;
    if (!settings) {
      return null;
    }
    return {
      name: settings.prompt_profile.name,
      global_prompt: settings.prompt_profile.global_prompt,
      run_prompt: settings.prompt_profile.run_prompt,
      summary_prompt: settings.prompt_profile.summary_prompt,
      dream_prompt: settings.prompt_profile.dream_prompt,
    };
  }

  function applyPromptConfig(name: string) {
    if (name === CURRENT_PROMPT_CONFIG_VALUE || reloadRequired) {
      return;
    }
    if (stageDraft !== null || globalDraft !== null) {
      toast.error("有未保存的修改", {
        description: "请先保存或刷新放弃修改，再切换提示词配置。",
      });
      return;
    }
    const config = localPromptConfigs.find((item) => item.name === name);
    if (!config) {
      return;
    }
    applyPromptConfigMutation.mutate(config);
  }

  function openSaveConfigDialog() {
    const current = buildCurrentPromptConfig();
    setSaveConfigName(current?.name ?? "");
    setSaveConfigDialogOpen(true);
  }

  function saveCurrentAsPromptConfig() {
    const current = buildCurrentPromptConfig();
    const name = saveConfigName.trim();
    if (!current || !name) {
      return;
    }
    if (name === CURRENT_PROMPT_CONFIG_VALUE) {
      toast.error("该名称为保留名称，请更换配置名称");
      return;
    }
    const exists = localPromptConfigs.some((item) => item.name === name);
    savePromptConfig({ ...current, name });
    setLocalPromptConfigs(listPromptConfigs());
    setSaveConfigDialogOpen(false);
    toast.success(exists ? `提示词配置「${name}」已更新` : `提示词配置「${name}」已保存`, {
      description: "可在上方下拉框中随时切换。",
    });
  }

  function exportCurrentPromptConfig() {
    const current = buildCurrentPromptConfig();
    if (!current) {
      return;
    }
    downloadConfig(current);
  }

  function handleImportConfigFile(file: File | null) {
    if (!file) {
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const content = reader.result;
        if (typeof content !== "string") {
          throw new Error("无法读取文件内容");
        }
        const parsed = parseImportedConfig(JSON.parse(content));
        const config =
          parsed.name === CURRENT_PROMPT_CONFIG_VALUE ? { ...parsed, name: "导入配置" } : parsed;
        if (reloadRequired) {
          toast.error("配置已被其他会话更新", { description: "请重新加载后再导入。" });
          return;
        }
        if (stageDraft !== null || globalDraft !== null) {
          toast.error("有未保存的修改", {
            description: "请先保存或刷新放弃修改，再导入提示词配置。",
          });
          return;
        }
        savePromptConfig(config);
        setLocalPromptConfigs(listPromptConfigs());
        applyPromptConfigMutation.mutate(config, {
          onSuccess: () => {
            setSelectedPromptConfig(config.name);
            toast.success(`提示词配置「${config.name}」已导入并应用`);
          },
        });
      } catch (error) {
        toast.error("导入提示词配置失败", {
          description: errorMessage(error, "文件内容格式不正确。"),
        });
      }
    };
    reader.readAsText(file);
  }

  if (settingsQuery.isLoading || channelsQuery.isLoading) {
    return <QueryLoadingState label="正在加载流程设置…" />;
  }
  if (settingsQuery.isError && !settingsQuery.data) {
    return (
      <QueryErrorState
        title="无法加载流程设置"
        error={settingsQuery.error}
        onRetry={() => void settingsQuery.refetch()}
      />
    );
  }
  if (channelsQuery.isError) {
    return (
      <QueryErrorState
        title="无法加载模型通道"
        error={channelsQuery.error}
        onRetry={() => void channelsQuery.refetch()}
      />
    );
  }
  if (!settingsQuery.data || !effectiveGlobalDraft) return null;

  const isGlobal = activeTab === GLOBAL_TAB_ID;
  const activeTitle = isGlobal
    ? "全局设置"
    : (activeMeta?.label ?? activeStage?.stage_id ?? "阶段设置");
  const activeDescription = isGlobal
    ? "统一定义两个阶段都会遵循的角色约束。"
    : (activeMeta?.description ?? "配置该阶段的模型、参数和提示词。");
  const isSaving = saveStageMutation.isPending || saveGlobalMutation.isPending;
  const ActiveStageIcon = isGlobal ? Settings2 : (activeMeta?.icon ?? SlidersHorizontal);

  return (
    <section className="h-full min-h-0 overflow-hidden" aria-label="阶段设置内容">
      <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[11rem_minmax(0,1fr)] xl:gap-12">
        <aside className="top-0 h-fit xl:sticky">
          <nav className="space-y-1 p-1" aria-label="流程设置导航" role="tablist">
            <Button
              id="stage-tab-Global"
              type="button"
              role="tab"
              aria-selected={isGlobal}
              aria-controls="stage-settings-panel"
              variant="ghost"
              className={cn(
                "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground active:bg-sidebar-accent active:text-sidebar-accent-foreground h-9 w-full justify-start gap-2 rounded-md px-3",
                isGlobal && "bg-sidebar-accent text-sidebar-accent-foreground",
              )}
              onClick={selectGlobal}
            >
              <Settings2 className="size-4 shrink-0" />
              <span>全局选项</span>
            </Button>
            {stages.map((stage) => {
              const meta = stageMetaById(stage.stage_id);
              const StageIcon = meta?.icon ?? SlidersHorizontal;
              const selected = activeTab === stage.stage_id;
              return (
                <Button
                  key={stage.stage_id}
                  id={`stage-tab-${stage.stage_id}`}
                  type="button"
                  role="tab"
                  aria-selected={selected}
                  aria-controls="stage-settings-panel"
                  variant="ghost"
                  className={cn(
                    "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground active:bg-sidebar-accent active:text-sidebar-accent-foreground h-9 w-full justify-start gap-2 rounded-md px-3",
                    selected && "bg-sidebar-accent text-sidebar-accent-foreground",
                  )}
                  onClick={() => selectStage(stage.stage_id)}
                >
                  <StageIcon className="size-4 shrink-0" />
                  <span>{meta?.label ?? stage.stage_id}</span>
                </Button>
              );
            })}
          </nav>
        </aside>

        <Card
          id="stage-settings-panel"
          role="tabpanel"
          aria-labelledby={`stage-tab-${activeTab}`}
          className="h-full min-h-0 gap-2 overflow-hidden py-4"
        >
          <CardHeader className="bg-background flex-none !gap-1.5 border-b !pb-1">
            <div className="flex items-start gap-3">
              <div key={activeTab} className="text-primary pt-0.5">
                <ActiveStageIcon className="size-5" />
              </div>
              <div key={`${activeTab}-text`} className="min-w-0">
                <CardTitle>{activeTitle}</CardTitle>
                <CardDescription className="mt-1">{activeDescription}</CardDescription>
              </div>
            </div>
          </CardHeader>

          {isGlobal ? (
            <CardContent
              key={activeTab}
              className="min-h-0 flex-1 space-y-5 overflow-y-auto pt-2 pb-6"
            >
              <section className="flex flex-col gap-3" aria-label="全局模型">
                <SectionLabel icon={<CpuIcon className="size-3.5" />}>全局模型</SectionLabel>
                <div className="grid gap-4 sm:grid-cols-[minmax(0,1fr)_12rem]">
                  <Field>
                    <FieldLabel htmlFor="global-model">统一设置两个阶段的模型</FieldLabel>
                    <Select
                      value={effectiveGlobalDraft.globalModelId}
                      onValueChange={selectGlobalModel}
                    >
                      <SelectTrigger id="global-model" className="w-full">
                        <SelectValue
                          placeholder={
                            configuredChannels.length === 0
                              ? "暂无可用模型"
                              : "各阶段模型不一致，选择后统一"
                          }
                        />
                      </SelectTrigger>
                      <SelectContent>
                        {configuredChannels.flatMap((channel) =>
                          channel.selected_models.map((model) => (
                            <SelectItem
                              key={model.selected_model_id}
                              value={String(model.selected_model_id)}
                            >
                              {channel.name} / {model.model_name}
                            </SelectItem>
                          )),
                        )}
                      </SelectContent>
                    </Select>
                    <FieldDescription>
                      保存后执行和总结阶段将统一使用该模型；之后仍可分别调整。
                    </FieldDescription>
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="global-thinking-effort">思考强度</FieldLabel>
                    <Select
                      value={effectiveGlobalDraft.globalThinkingEffort}
                      disabled={!effectiveGlobalDraft.globalModelId}
                      onValueChange={(globalThinkingEffort) =>
                        updateGlobalDraft({ globalThinkingEffort })
                      }
                    >
                      <SelectTrigger id="global-thinking-effort" className="w-full">
                        <SelectValue
                          placeholder={
                            effectiveGlobalDraft.globalModelId &&
                            !effectiveGlobalDraft.globalThinkingEffort
                              ? "各阶段强度不一致"
                              : "模型默认"
                          }
                        />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value={DEFAULT_THINKING_EFFORT_VALUE}>模型默认</SelectItem>
                        {thinkingEffortsForModel(effectiveGlobalDraft.globalModelId).map(
                          (effort) => (
                            <SelectItem key={effort} value={effort}>
                              {thinkingEffortLabel(effort)}
                            </SelectItem>
                          ),
                        )}
                      </SelectContent>
                    </Select>
                  </Field>
                </div>
              </section>

              <section className="flex flex-col gap-3" aria-label="提示词配置">
                <SectionLabel icon={<LayersIcon className="size-3.5" />}>提示词配置</SectionLabel>
                <div className="flex flex-wrap items-end gap-3">
                  <Field className="min-w-0 flex-1">
                    <FieldLabel htmlFor="prompt-config-select">切换提示词配置</FieldLabel>
                    <Select
                      value={selectedPromptConfig}
                      onValueChange={applyPromptConfig}
                      disabled={applyPromptConfigMutation.isPending}
                    >
                      <SelectTrigger id="prompt-config-select" className="w-full">
                        <SelectValue placeholder="选择一套配置" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value={CURRENT_PROMPT_CONFIG_VALUE}>
                          默认配置（当前生效）
                        </SelectItem>
                        {localPromptConfigs.map((config) => (
                          <SelectItem key={config.name} value={config.name}>
                            {config.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </Field>
                  <div className="flex flex-wrap gap-2 pb-0.5">
                    <Button variant="outline" size="sm" onClick={openSaveConfigDialog}>
                      保存为配置
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={exportCurrentPromptConfig}
                      disabled={!settingsQuery.data}
                    >
                      导出
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => configFileInputRef.current?.click()}
                    >
                      导入
                    </Button>
                    <input
                      ref={configFileInputRef}
                      type="file"
                      accept="application/json,.json"
                      className="hidden"
                      onChange={(event) => {
                        handleImportConfigFile(event.target.files?.[0] ?? null);
                        event.target.value = "";
                      }}
                    />
                  </div>
                </div>
                <FieldDescription>
                  全局系统提示词与两个阶段的提示词会作为一整套配置保存在本机浏览器， 也可以导出为
                  JSON 文件或从文件导入。
                </FieldDescription>
              </section>

              <section className="flex flex-col gap-3" aria-label="全局提示词">
                <SectionLabel icon={<ScrollTextIcon className="size-3.5" />}>提示词</SectionLabel>
                <Field>
                  <FieldLabel htmlFor="global-system-prompt">全局系统提示词</FieldLabel>
                  <Textarea
                    id="global-system-prompt"
                    value={effectiveGlobalDraft.globalPrompt}
                    onChange={(event) => updateGlobalDraft({ globalPrompt: event.target.value })}
                    placeholder="定义所有阶段都必须遵循的角色、边界与输出原则…"
                    className="min-h-40 resize-y text-sm"
                  />
                  <FieldDescription>
                    这段规则会自动附加到执行和总结两个阶段的提示词之前。
                  </FieldDescription>
                </Field>
              </section>
            </CardContent>
          ) : !activeStage || !effectiveStageDraft ? (
            <CardContent
              key={activeTab}
              className="text-muted-foreground min-h-0 flex-1 overflow-y-auto py-12 text-center text-sm"
            >
              该阶段尚未配置。请刷新设置后重试。
            </CardContent>
          ) : (
            <CardContent
              key={activeTab}
              className="min-h-0 flex-1 space-y-5 overflow-y-auto pt-2 pb-6"
            >
              <section className="flex flex-col gap-3" aria-label="阶段模型与参数">
                <SectionLabel icon={<CpuIcon className="size-3.5" />}>模型与参数</SectionLabel>
                <div className="grid gap-4 sm:grid-cols-[minmax(0,1fr)_12rem]">
                  <Field>
                    <FieldLabel htmlFor="stage-model">模型</FieldLabel>
                    <Select value={effectiveStageDraft.modelId} onValueChange={selectStageModel}>
                      <SelectTrigger id="stage-model" className="w-full">
                        <SelectValue placeholder="选择一个已配置的模型" />
                      </SelectTrigger>
                      <SelectContent>
                        {configuredChannels.flatMap((channel) =>
                          channel.selected_models.map((model) => (
                            <SelectItem
                              key={model.selected_model_id}
                              value={String(model.selected_model_id)}
                            >
                              {channel.name} / {model.model_name}
                            </SelectItem>
                          )),
                        )}
                      </SelectContent>
                    </Select>
                    {configuredChannels.length === 0 && (
                      <FieldDescription>
                        暂无可用模型，请先在「主要设置 · 渠道模型」中添加。
                      </FieldDescription>
                    )}
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="stage-thinking-effort">思考强度</FieldLabel>
                    <Select
                      value={effectiveStageDraft.thinkingEffort}
                      disabled={!effectiveStageDraft.modelId}
                      onValueChange={(thinkingEffort) => updateDraft({ thinkingEffort })}
                    >
                      <SelectTrigger id="stage-thinking-effort" className="w-full">
                        <SelectValue placeholder="模型默认" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value={DEFAULT_THINKING_EFFORT_VALUE}>模型默认</SelectItem>
                        {thinkingEffortsForModel(effectiveStageDraft.modelId).map((effort) => (
                          <SelectItem key={effort} value={effort}>
                            {thinkingEffortLabel(effort)}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </Field>
                </div>
                <div className="grid gap-4 sm:grid-cols-2">
                  <Field>
                    <FieldLabel htmlFor="stage-temperature">随机性（Temperature）</FieldLabel>
                    <Input
                      id="stage-temperature"
                      type="number"
                      min={0}
                      max={2}
                      step={0.1}
                      value={effectiveStageDraft.temperature}
                      onChange={(event) => updateDraft({ temperature: event.target.value })}
                    />
                    <FieldDescription>控制输出的随机性，范围 0–2，默认值 0。</FieldDescription>
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="stage-top-p">候选词范围（Top P）</FieldLabel>
                    <Input
                      id="stage-top-p"
                      type="number"
                      min={0}
                      max={1}
                      step={0.05}
                      value={effectiveStageDraft.topP}
                      onChange={(event) => updateDraft({ topP: event.target.value })}
                    />
                    <FieldDescription>控制候选词范围，范围 0–1，默认值 1。</FieldDescription>
                  </Field>
                </div>
              </section>

              {activeStage.stage_id === "Dream" ? (
                <section className="flex flex-col gap-3" aria-label="梦境运行时间">
                  <SectionLabel icon={<MoonIcon className="size-3.5" />}>运行时间</SectionLabel>
                  <Field className="max-w-xs">
                    <FieldLabel htmlFor="dream-schedule-time">每日执行时间</FieldLabel>
                    <Input
                      id="dream-schedule-time"
                      type="time"
                      step={60}
                      value={effectiveStageDraft.dreamScheduleTime}
                      onChange={(event) => updateDraft({ dreamScheduleTime: event.target.value })}
                    />
                    <FieldDescription>
                      按上海时间每天执行一次，整理前一天的运行报告与长期记忆。
                    </FieldDescription>
                  </Field>
                </section>
              ) : null}

              <section className="flex flex-col gap-3" aria-label="阶段提示词">
                <SectionLabel icon={<ScrollTextIcon className="size-3.5" />}>提示词</SectionLabel>
                <Field>
                  <FieldLabel htmlFor="stage-prompt">阶段系统提示词</FieldLabel>
                  <Textarea
                    id="stage-prompt"
                    value={effectiveStageDraft.prompt}
                    onChange={(event) => updateDraft({ prompt: event.target.value })}
                    placeholder="输入仅适用于该阶段的任务说明与输出要求…"
                    className="min-h-40 resize-y text-sm"
                  />
                  <FieldDescription>
                    这里仅填写该阶段的专属指令；所有阶段共用的角色与边界请在“全局”中配置。
                  </FieldDescription>
                </Field>
              </section>
            </CardContent>
          )}

          <div className="border-border/60 flex flex-none justify-end border-t px-1 pt-3">
            {reloadRequired ? (
              <Button type="button" variant="outline" onClick={() => void reloadSettings()}>
                重新加载
              </Button>
            ) : (
              <Button
                type="button"
                onClick={isGlobal ? saveGlobal : saveStage}
                disabled={isSaving || (!isGlobal && (!activeStage || !effectiveStageDraft))}
              >
                {isSaving ? <Spinner className="size-4" /> : <Save className="size-4" />}
                {isSaving ? "保存中…" : isGlobal ? "保存全局设置" : "保存阶段设置"}
              </Button>
            )}
          </div>
        </Card>
      </div>

      <Dialog open={saveConfigDialogOpen} onOpenChange={setSaveConfigDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>保存为提示词配置</DialogTitle>
            <DialogDescription>
              将当前全局系统提示词与两个阶段提示词保存为一套命名配置。
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-2">
            <Label htmlFor="prompt-config-name" className="sr-only">
              配置名称
            </Label>
            <Input
              id="prompt-config-name"
              value={saveConfigName}
              onChange={(event) => setSaveConfigName(event.target.value)}
              placeholder="输入配置名称，如：稳健型、激进型"
            />
          </div>
          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">
                取消
              </Button>
            </DialogClose>
            <Button
              type="button"
              disabled={!saveConfigName.trim()}
              onClick={saveCurrentAsPromptConfig}
            >
              保存配置
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  );
}
