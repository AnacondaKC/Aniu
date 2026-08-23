import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { MainSettingsLayout } from "./main-settings-layout";

vi.mock("@/features/settings/components/mx-settings-section", () => ({
  MxSettingsPage: () => <div>妙想设置内容</div>,
}));
vi.mock("@/features/settings/model-channels-settings-page", () => ({
  ModelChannelsSettingsPage: () => <div>渠道模型内容</div>,
}));
vi.mock("@/features/settings/schedules-page", () => ({
  TradingSchedulesPage: () => <div>交易任务内容</div>,
}));

describe("MainSettingsLayout", () => {
  it("renders settings sections as tabs without secondary routes", async () => {
    const user = userEvent.setup();
    render(<MainSettingsLayout />);

    const mxTab = screen.getByRole("tab", { name: "妙想设置" });
    const channelsTab = screen.getByRole("tab", { name: "渠道模型" });
    expect(screen.queryByRole("tab", { name: "复盘任务" })).not.toBeInTheDocument();
    expect(mxTab).toHaveAttribute("aria-selected", "true");
    expect(screen.getByText("妙想设置内容")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "妙想设置" })).not.toBeInTheDocument();

    await user.click(channelsTab);

    expect(channelsTab).toHaveAttribute("aria-selected", "true");
    expect(mxTab).toHaveAttribute("aria-selected", "false");
    expect(screen.getByText("渠道模型内容")).toBeInTheDocument();
  });
});
