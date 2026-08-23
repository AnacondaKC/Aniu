import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";

import {
  setupIdentity,
  fetchAuthSession,
  login as loginRequest,
  logout as logoutRequest,
} from "@/lib/api";
import { getAuthSession, subscribeAuthSession, type AuthSessionState } from "@/lib/auth-session";
import { AuthContext, type AuthContextValue } from "@/features/auth/auth-context";
import { Spinner } from "@/components/ui/spinner";
import { Button } from "@/components/ui/button";

let sessionProbe: Promise<void> | null = null;

function probeAuthSession() {
  if (sessionProbe === null) {
    const probe = fetchAuthSession().then(() => undefined);
    sessionProbe = probe;
    const clear = () => {
      if (sessionProbe === probe) {
        sessionProbe = null;
      }
    };
    void probe.then(clear, clear);
  }
  return sessionProbe;
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<AuthSessionState>(() => getAuthSession());
  const [loading, setLoading] = useState(true);
  const [probeError, setProbeError] = useState(false);
  const [probeAttempt, setProbeAttempt] = useState(0);

  useEffect(() => subscribeAuthSession(() => setSession(getAuthSession())), []);

  const refresh = useCallback(async () => {
    await fetchAuthSession();
  }, []);

  useEffect(() => {
    let cancelled = false;
    let retryTimer: number | null = null;
    void probeAuthSession().then(
      () => {
        if (!cancelled) {
          setProbeError(false);
          setLoading(false);
        }
      },
      () => {
        if (!cancelled) {
          setProbeError(true);
          const delay = Math.min(1_000 * 2 ** probeAttempt, 5_000);
          retryTimer = window.setTimeout(() => {
            setProbeAttempt((attempt) => attempt + 1);
          }, delay);
        }
      },
    );
    return () => {
      cancelled = true;
      if (retryTimer !== null) {
        window.clearTimeout(retryTimer);
      }
    };
  }, [probeAttempt]);

  const value = useMemo<AuthContextValue>(
    () => ({
      ...session,
      loading,
      refresh,
      login: async (username, password) => {
        await loginRequest(username, password);
      },
      setup: async (username, password) => {
        await setupIdentity(username, password);
      },
      logout: async () => {
        await logoutRequest();
      },
    }),
    [session, loading, refresh],
  );

  if (probeError) {
    return (
      <div className="text-muted-foreground flex min-h-svh flex-col items-center justify-center gap-3">
        <p role="alert">无法连接服务</p>
        <Button
          type="button"
          variant="outline"
          onClick={() => setProbeAttempt((attempt) => attempt + 1)}
        >
          重试
        </Button>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="text-muted-foreground flex min-h-svh items-center justify-center">
        <Spinner />
      </div>
    );
  }

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
