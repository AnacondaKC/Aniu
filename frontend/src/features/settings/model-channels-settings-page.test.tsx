import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ModelChannelsSettingsPage } from "./model-channels-settings-page";

const api = vi.hoisted(() => ({
  clearModelChannelApiKey: vi.fn(),
  createModelChannel: vi.fn(),
  deleteModelChannel: vi.fn(),
  fetchModelCatalog: vi.fn(),
  listModelChannels: vi.fn(),
  lookupModelsDevModel: vi.fn(),
  updateModelChannel: vi.fn(),
}));

vi.mock("@/lib/api", () => api);
vi.mock("@/features/settings/model-channels", () => ({
  ModelSettingsCard: () => <section aria-label="渠道模型">渠道模型</section>,
}));

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <ModelChannelsSettingsPage />
    </QueryClientProvider>,
  );
}

afterEach(() => vi.clearAllMocks());

describe("ModelChannelsSettingsPage", () => {
  it("only renders model-channel management", async () => {
    api.listModelChannels.mockResolvedValue([]);

    renderPage();

    expect(await screen.findByRole("region", { name: "渠道模型" })).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "妙想设置内容" })).not.toBeInTheDocument();
  });
});
