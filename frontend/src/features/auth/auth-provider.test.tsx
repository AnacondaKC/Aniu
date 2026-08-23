import { act, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { setAuthSession } from "@/lib/auth-session";
import { AuthProvider } from "./auth-provider";

const apiMocks = vi.hoisted(() => ({
  fetchAuthSession: vi.fn(),
  login: vi.fn(),
  logout: vi.fn(),
  setupIdentity: vi.fn(),
}));

vi.mock("@/lib/api", () => apiMocks);

describe("AuthProvider", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
    setAuthSession({
      authenticated: false,
      identityInitialized: false,
      username: null,
      csrfToken: null,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("does not enter open mode after a failed probe and retries automatically", async () => {
    apiMocks.fetchAuthSession
      .mockRejectedValueOnce(new Error("backend is starting"))
      .mockImplementationOnce(() => {
        setAuthSession({
          authenticated: true,
          identityInitialized: true,
          username: "aniu",
          csrfToken: "stable-token",
        });
        return Promise.resolve();
      });

    render(
      <AuthProvider>
        <div>authenticated application</div>
      </AuthProvider>,
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByRole("alert")).toHaveTextContent("无法连接服务");
    expect(screen.queryByText("authenticated application")).not.toBeInTheDocument();

    await act(async () => {
      vi.advanceTimersByTime(1_000);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(apiMocks.fetchAuthSession).toHaveBeenCalledTimes(2);
    expect(screen.getByText("authenticated application")).toBeInTheDocument();
  });
});
