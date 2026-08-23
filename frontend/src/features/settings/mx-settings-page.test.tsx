import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MxSettingsPage } from "./components/mx-settings-section";

const api = vi.hoisted(() => ({
  getSettings: vi.fn(),
  updateSettings: vi.fn(),
}));

vi.mock("@/lib/api", () => api);

const settings = {
  mx: { api_key_configured: false, api_key_last_four: null },
  prompt_profile: {
    schema: "aniu.prompt-profile.v3",
    name: "默认",
    description: "",
    global_prompt: "",
    run_prompt: "",
    summary_prompt: "",
  },
  stage_settings: [],
  revision: 4,
  created_at: "2026-08-03T08:00:00Z",
  updated_at: "2026-08-03T08:00:00Z",
} as const;

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MxSettingsPage />
    </QueryClientProvider>,
  );
}

afterEach(() => vi.clearAllMocks());

describe("MxSettingsPage", () => {
  it("places saving immediately after clearing for configured keys", async () => {
    api.getSettings.mockResolvedValue({
      ...settings,
      mx: { api_key_configured: true, api_key_last_four: "7890" },
    });

    renderPage();

    const clearButton = await screen.findByRole("button", { name: "清除" });
    const saveButton = screen.getByRole("button", { name: "保存密钥" });
    expect(
      clearButton.compareDocumentPosition(saveButton) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });

  it("saves the key through general settings", async () => {
    const user = userEvent.setup();
    api.getSettings.mockResolvedValue(settings);
    api.updateSettings.mockResolvedValue({
      ...settings,
      mx: { api_key_configured: true, api_key_last_four: "7890" },
      revision: 5,
    });

    renderPage();

    expect(await screen.findByRole("region", { name: "妙想设置内容" })).toBeInTheDocument();
    await user.type(screen.getByLabelText("妙想 API 密钥"), "mx-secret-7890");
    await user.click(screen.getByRole("button", { name: "保存密钥" }));

    await waitFor(() => {
      expect(api.updateSettings.mock.calls[0]?.[0]).toEqual({
        expected_revision: 4,
        mx_api_key: "mx-secret-7890",
      });
    });
  });
});
