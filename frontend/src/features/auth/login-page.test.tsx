import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AuthContext, type AuthContextValue } from "@/features/auth/auth-context";
import { LoginPage } from "./login-page";

const apiMocks = vi.hoisted(() => ({
  setupIdentity: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("@/lib/api", () => apiMocks);

function renderLogin(identityInitialized: boolean) {
  const auth: AuthContextValue = {
    authenticated: false,
    identityInitialized,
    username: null,
    csrfToken: null,
    loading: false,
    refresh: vi.fn().mockResolvedValue(undefined),
    login: vi.fn().mockResolvedValue(undefined),
    logout: vi.fn().mockResolvedValue(undefined),
  };

  render(
    <AuthContext.Provider value={auth}>
      <MemoryRouter initialEntries={["/login"]}>
        <LoginPage />
      </MemoryRouter>
    </AuthContext.Provider>,
  );
  return auth;
}

describe("LoginPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("uses only a token for first-run setup", async () => {
    const auth = renderLogin(false);
    const user = userEvent.setup();

    expect(screen.queryByLabelText("用户名")).not.toBeInTheDocument();
    expect(screen.getByLabelText("访问 Token")).toHaveAttribute("minlength", "8");
    await user.type(screen.getByLabelText("访问 Token"), " first-token ");
    await user.click(screen.getByRole("button", { name: "保存并登录" }));

    expect(apiMocks.setupIdentity).toHaveBeenCalledWith("first-token");
    expect(auth.login).not.toHaveBeenCalled();
  });

  it("submits the token to login after setup", async () => {
    const auth = renderLogin(true);
    const user = userEvent.setup();

    await user.type(screen.getByLabelText("访问 Token"), "session-token");
    await user.click(screen.getByRole("button", { name: "登录" }));

    expect(auth.login).toHaveBeenCalledWith("session-token");
    expect(apiMocks.setupIdentity).not.toHaveBeenCalled();
  });
});
