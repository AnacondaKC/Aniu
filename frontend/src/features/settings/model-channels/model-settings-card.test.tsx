import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import type { ModelProfile } from "@/lib/api-types";

import { ModelSettingsCard } from "./model-settings-card";

class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}

vi.stubGlobal("ResizeObserver", ResizeObserverMock);

Object.defineProperties(HTMLElement.prototype, {
  hasPointerCapture: { configurable: true, value: () => false },
  releasePointerCapture: { configurable: true, value: () => undefined },
  scrollIntoView: { configurable: true, value: () => undefined },
  setPointerCapture: { configurable: true, value: () => undefined },
});

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
  selected_models: [],
  provider_config: { auth_mode: "auto" },
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-02T00:00:00Z",
};

function renderCard({
  onClearApiKey = vi.fn().mockResolvedValue(channel),
}: {
  onClearApiKey?: (channelId: number, revision: number) => Promise<ModelProfile>;
} = {}) {
  const onSaveChannel = vi.fn().mockResolvedValue({ ...channel, revision: 13 });
  render(
    <ModelSettingsCard
      channels={[channel]}
      onSaveChannel={onSaveChannel}
      onDeleteChannel={vi.fn()}
      onClearApiKey={onClearApiKey}
      onFetchModels={vi.fn()}
      onLookupModelsDev={vi.fn()}
    />,
  );
  return { onSaveChannel, onClearApiKey };
}

describe("ModelSettingsCard", () => {
  it("closes the model dialog after adding a valid manual model", async () => {
    const user = userEvent.setup();
    const { onSaveChannel } = renderCard();

    await user.click(screen.getByRole("button", { name: /Primary/ }));
    await user.click(screen.getByRole("button", { name: "添加模型" }));
    await user.type(screen.getByRole("textbox", { name: "模型名称" }), "gpt-5.5");
    await user.click(screen.getByRole("button", { name: "添加并保存" }));

    await waitFor(() => expect(onSaveChannel).toHaveBeenCalledOnce());
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  });

  it("keeps the configured key when clearing fails", async () => {
    const user = userEvent.setup();
    const onClearApiKey = vi.fn().mockRejectedValue(new Error("network error"));
    const result = renderCard({ onClearApiKey });

    await user.click(screen.getByRole("button", { name: /Primary/ }));
    await user.click(screen.getByRole("button", { name: "清除密钥" }));

    await waitFor(() => expect(onClearApiKey).toHaveBeenCalledWith(7, 12));
    expect(result.onClearApiKey).toHaveBeenCalledOnce();
    expect(screen.getByRole("button", { name: "清除密钥" })).toBeInTheDocument();
  });
});
