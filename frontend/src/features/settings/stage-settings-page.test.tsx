import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiConflictError } from "@/lib/openapi-client";
import { StageSettingsPage } from "./stage-settings-page";

const api = vi.hoisted(() => ({
  getSettings: vi.fn(),
  listModelChannels: vi.fn(),
  updateSettings: vi.fn(),
}));

vi.mock("@/lib/api", () => api);

const settings = {
  revision: 3,
  dream_schedule_time: "00:30",
  mx: { api_key_configured: false, api_key_last_four: null },
  prompt_profile: {
    schema: "aniu.prompt-profile.v3",
    name: "默认提示词配置",
    description: "",
    global_prompt: "原全局规则",
    run_prompt: "执行提示词",
    summary_prompt: "总结提示词",
    dream_prompt: "梦境提示词",
  },
  stage_settings: [
    {
      stage_id: "Run",
      model_selected_model_id: null,
      temperature: 0,
      top_p: 1,
      thinking_effort: null,
      prompt: "执行提示词",
    },
    {
      stage_id: "Summary",
      model_selected_model_id: null,
      temperature: 0,
      top_p: 1,
      thinking_effort: null,
      prompt: "总结提示词",
    },
    {
      stage_id: "Dream",
      model_selected_model_id: null,
      temperature: 0,
      top_p: 1,
      thinking_effort: null,
      prompt: "梦境提示词",
    },
  ],
};

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <StageSettingsPage />
    </QueryClientProvider>,
  );
}

afterEach(() => vi.clearAllMocks());

describe("StageSettingsPage global settings", () => {
  it("saves the shared prompt", async () => {
    const user = userEvent.setup();
    api.getSettings.mockResolvedValue(settings);
    api.listModelChannels.mockResolvedValue([]);
    api.updateSettings.mockResolvedValue({
      ...settings,
      revision: 4,
    });

    renderPage();

    const globalPrompt = await screen.findByLabelText("全局系统提示词");
    expect(screen.queryByLabelText("最大调用轮数")).not.toBeInTheDocument();
    await user.clear(globalPrompt);
    await user.type(globalPrompt, "新的全局规则");
    await user.click(screen.getByRole("button", { name: "保存全局设置" }));

    await waitFor(() => expect(api.updateSettings).toHaveBeenCalledTimes(1));
    expect(api.updateSettings).toHaveBeenCalledWith(
      expect.objectContaining({
        expected_revision: 3,
        prompt_profile: expect.objectContaining({ global_prompt: "新的全局规则" }),
      }),
    );
  });

  it("synchronizes the selected thinking effort to both stages", async () => {
    const user = userEvent.setup();
    const configuredSettings = {
      ...settings,
      stage_settings: settings.stage_settings.map((stage) => ({
        ...stage,
        model_selected_model_id: 1,
      })),
    };
    api.getSettings.mockResolvedValue(configuredSettings);
    api.listModelChannels.mockResolvedValue([
      {
        name: "测试通道",
        enabled: true,
        selected_models: [
          {
            selected_model_id: 1,
            model_name: "测试模型",
            thinking_efforts: ["low", "high"],
          },
        ],
      },
    ]);
    api.updateSettings.mockResolvedValue({ ...configuredSettings, revision: 4 });

    renderPage();

    await user.click(await screen.findByLabelText("思考强度"));
    await user.click(await screen.findByRole("option", { name: "高" }));
    await user.click(screen.getByRole("button", { name: "保存全局设置" }));

    await waitFor(() => expect(api.updateSettings).toHaveBeenCalledTimes(1));
    expect(api.updateSettings.mock.calls[0]?.[0].stage_settings).toEqual(
      expect.arrayContaining(
        configuredSettings.stage_settings.map((stage) =>
          expect.objectContaining({
            stage_id: stage.stage_id,
            model_selected_model_id: 1,
            thinking_effort: "high",
          }),
        ),
      ),
    );
  });

  it("keeps edits made while a global save is pending", async () => {
    const user = userEvent.setup();
    let resolveUpdate!: (value: typeof settings) => void;
    api.getSettings.mockResolvedValue(settings);
    api.listModelChannels.mockResolvedValue([]);
    api.updateSettings.mockReturnValue(
      new Promise<typeof settings>((resolve) => {
        resolveUpdate = resolve;
      }),
    );

    renderPage();

    const globalPrompt = await screen.findByLabelText("全局系统提示词");
    await user.clear(globalPrompt);
    await user.type(globalPrompt, "已提交的规则");
    await user.click(screen.getByRole("button", { name: "保存全局设置" }));
    await waitFor(() => expect(api.updateSettings).toHaveBeenCalledTimes(1));

    await user.clear(globalPrompt);
    await user.type(globalPrompt, "请求期间的新规则");
    await act(async () => {
      resolveUpdate({
        ...settings,
        revision: 4,
        prompt_profile: { ...settings.prompt_profile, global_prompt: "已提交的规则" },
      });
      await Promise.resolve();
    });

    expect(globalPrompt).toHaveValue("请求期间的新规则");
  });

  it("saves the Dream execution time with the Dream stage", async () => {
    const user = userEvent.setup();
    const configuredSettings = {
      ...settings,
      stage_settings: settings.stage_settings.map((stage) =>
        stage.stage_id === "Dream" ? { ...stage, model_selected_model_id: 1 } : stage,
      ),
    };
    api.getSettings.mockResolvedValue(configuredSettings);
    api.listModelChannels.mockResolvedValue([
      {
        name: "测试通道",
        enabled: true,
        selected_models: [{ selected_model_id: 1, model_name: "测试模型", thinking_efforts: [] }],
      },
    ]);
    api.updateSettings.mockResolvedValue({
      ...configuredSettings,
      revision: 4,
      dream_schedule_time: "04:15",
    });

    renderPage();

    await user.click(await screen.findByRole("tab", { name: "梦境阶段" }));
    const timeInput = await screen.findByLabelText("每日执行时间");
    expect(timeInput).toHaveValue("00:30");
    await user.clear(timeInput);
    await user.type(timeInput, "04:15");
    await user.click(screen.getByRole("button", { name: "保存阶段设置" }));

    await waitFor(() => expect(api.updateSettings).toHaveBeenCalledTimes(1));
    expect(api.updateSettings.mock.calls[0]?.[0]).toEqual(
      expect.objectContaining({
        expected_revision: 3,
        dream_schedule_time: "04:15",
      }),
    );
  });

  it("does not expose report rendering switches", async () => {
    const user = userEvent.setup();
    api.getSettings.mockResolvedValue(settings);
    api.listModelChannels.mockResolvedValue([]);

    renderPage();

    await user.click(await screen.findByRole("tab", { name: "总结阶段" }));
    expect(screen.queryByLabelText("使用 HTML 渲染总结报告")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("HTML 渲染提示词")).not.toBeInTheDocument();
    expect(await screen.findByLabelText("阶段系统提示词")).toHaveValue("总结提示词");
  });

  it("keeps the conflict and draft when server reload fails", async () => {
    const user = userEvent.setup();
    api.getSettings
      .mockResolvedValueOnce(settings)
      .mockRejectedValueOnce(new Error("reload failed"));
    api.listModelChannels.mockResolvedValue([]);
    api.updateSettings.mockRejectedValue(
      new ApiConflictError(
        "conflict",
        {},
        {
          resource: "app_settings",
          expectedRevision: 3,
          actualRevision: 4,
          requestId: null,
        },
      ),
    );

    renderPage();

    const globalPrompt = await screen.findByLabelText("全局系统提示词");
    await user.clear(globalPrompt);
    await user.type(globalPrompt, "必须保留的草稿");
    await user.click(screen.getByRole("button", { name: "保存全局设置" }));
    const reload = await screen.findByRole("button", { name: "重新加载" });
    await user.click(reload);
    await waitFor(() => expect(api.getSettings).toHaveBeenCalledTimes(2));

    expect(screen.getByLabelText("全局系统提示词")).toHaveValue("必须保留的草稿");
  });
});

describe("StageSettingsPage prompt configs", () => {
  const PROMPT_CONFIGS_STORAGE_KEY = "aniu.prompt-configs.v3";

  afterEach(() => localStorage.clear());

  function seedLocalConfigs() {
    localStorage.setItem(
      PROMPT_CONFIGS_STORAGE_KEY,
      JSON.stringify([
        {
          name: "稳健型",
          global_prompt: "配置A全局",
          run_prompt: "配置A执行",
          summary_prompt: "配置A总结",
          dream_prompt: "配置A梦境",
        },
      ]),
    );
  }

  it("blocks switching a prompt config while drafts are unsaved", async () => {
    const user = userEvent.setup();
    seedLocalConfigs();
    api.getSettings.mockResolvedValue(settings);
    api.listModelChannels.mockResolvedValue([]);

    renderPage();

    await user.type(await screen.findByLabelText("全局系统提示词"), "未保存的草稿");
    act(() => {
      fireEvent.click(screen.getByRole("combobox", { name: "切换提示词配置" }));
    });
    const option = await screen.findByRole("option", { name: "稳健型" });
    await act(async () => {
      fireEvent.click(option);
      await Promise.resolve();
    });

    await waitFor(() => expect(option).not.toBeInTheDocument());
    expect(api.updateSettings).not.toHaveBeenCalled();
  });

  it("saves current three-stage prompts as a named config", async () => {
    const user = userEvent.setup();
    api.getSettings.mockResolvedValue(settings);
    api.listModelChannels.mockResolvedValue([]);

    renderPage();

    await user.click(await screen.findByRole("button", { name: "保存为配置" }));
    const nameInput = await screen.findByLabelText("配置名称");
    await user.clear(nameInput);
    await user.type(nameInput, "稳健型");
    await user.click(screen.getByRole("button", { name: "保存配置" }));

    const stored = JSON.parse(localStorage.getItem(PROMPT_CONFIGS_STORAGE_KEY) ?? "[]");
    expect(stored).toEqual([
      {
        name: "稳健型",
        global_prompt: "原全局规则",
        run_prompt: "执行提示词",
        summary_prompt: "总结提示词",
        dream_prompt: "梦境提示词",
      },
    ]);
  });
});
