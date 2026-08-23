import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { createEmptyChannelDraft } from "./channel-draft";
import { ModelPickerDialog } from "./model-picker-dialog";

Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
  configurable: true,
  value: () => undefined,
});

function renderDialog(overrides: Partial<Parameters<typeof ModelPickerDialog>[0]> = {}) {
  const draft = {
    ...createEmptyChannelDraft(0),
    name: "Primary",
    baseUrl: "https://api.example.test/v1",
    ...overrides.draft,
  };
  const props = {
    draft,
    editingModelKey: null,
    onOpenChange: vi.fn(),
    onFetchModels: vi.fn(),
    onManualNameChange: vi.fn(),
    onSelectCatalogModel: vi.fn(),
    onManualFieldChange: vi.fn(),
    onManualThinkingEffortsChange: vi.fn(),
    onLookupModelsDev: vi.fn(),
    onAddManualModel: vi.fn(),
    ...overrides,
  };
  render(<ModelPickerDialog {...props} />);
  return props;
}

describe("ModelPickerDialog", () => {
  it("fetches channel models when the official select is opened", () => {
    const props = renderDialog();
    const select = screen.getByRole("combobox", { name: "从渠道选择模型" });

    fireEvent.click(select);

    expect(props.onFetchModels).toHaveBeenCalledOnce();
    expect(screen.getByText("当前渠道没有可选模型")).toBeInTheDocument();
  });

  it("selects a fetched model from the official dropdown", () => {
    const props = renderDialog({
      draft: {
        ...createEmptyChannelDraft(0),
        name: "Primary",
        baseUrl: "https://api.example.test/v1",
        hasFetchedModels: true,
        fetchedModels: [
          { model: "gpt-5.5", label: "GPT-5.5", provider_id: "openai/gpt-5.5" },
          { model: "gpt-4.1", label: "GPT-4.1", provider_id: "openai/gpt-4.1" },
        ],
      },
    });
    const select = screen.getByRole("combobox", { name: "从渠道选择模型" });

    fireEvent.click(select);
    fireEvent.click(screen.getByRole("option", { name: "gpt-5.5" }));

    expect(props.onSelectCatalogModel).toHaveBeenCalledWith({
      model: "gpt-5.5",
      label: "GPT-5.5",
      provider_id: "openai/gpt-5.5",
    });
  });

  it("keeps the provider identity internal and labels every price input", () => {
    renderDialog();

    expect(screen.queryByRole("textbox", { name: "models.dev 模型标识" })).not.toBeInTheDocument();
    expect(screen.getByText("输入价格")).toBeVisible();
    expect(screen.getByText("输出价格")).toBeVisible();
    expect(screen.getByText("缓存读取价格")).toBeVisible();
    expect(screen.getByText("缓存写入价格")).toBeVisible();
  });

  it("emits the enabled thinking presets for the edited model", () => {
    const props = renderDialog();

    expect(screen.getByText("Available Thinking Efforts")).toBeVisible();
    expect(screen.getByText("High")).toBeVisible();
    fireEvent.click(screen.getByRole("checkbox", { name: "启用高思考强度" }));

    expect(props.onManualThinkingEffortsChange).toHaveBeenCalledWith(["high"]);
  });

  it("uses edit-specific title and submit text for an existing model", () => {
    renderDialog({ editingModelKey: "openai/gpt-5.5" });

    expect(screen.getByRole("heading", { name: "修改模型" })).toBeVisible();
    expect(screen.getByRole("button", { name: "保存修改" })).toBeVisible();
  });

  it("keeps an existing model editable when its stored limits need correction", () => {
    renderDialog({
      editingModelKey: "openai/gpt-5.5",
      draft: {
        ...createEmptyChannelDraft(0),
        name: "Primary",
        baseUrl: "https://api.example.test/v1",
        manualModelName: "gpt-5.5",
        manualContextWindowTokens: "1000",
        manualMaxOutputTokens: "2000",
      },
    });

    expect(screen.getByRole("button", { name: "保存修改" })).toBeEnabled();
    expect(screen.getByRole("alert")).toHaveTextContent("最大输出 Token 不能大于最大上下文 Token");
  });
});
