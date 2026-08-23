import { createContext, useContext } from "react";

import type { AuthSessionState } from "@/lib/auth-session";

export type AuthContextValue = AuthSessionState & {
  loading: boolean;
  refresh: () => Promise<void>;
  login: (token: string) => Promise<void>;
  logout: () => Promise<void>;
};

export const AuthContext = createContext<AuthContextValue | null>(null);

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider");
  }
  return context;
}
