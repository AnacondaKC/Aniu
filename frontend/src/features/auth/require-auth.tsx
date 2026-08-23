import { Navigate, Outlet, useLocation } from "react-router-dom";

import { useAuth } from "@/features/auth/auth-context";

/** Gate protected app routes after the local identity has been initialized. */
export function RequireAuth() {
  const auth = useAuth();
  const location = useLocation();

  if (!auth.identityInitialized || auth.authenticated) {
    return <Outlet />;
  }

  const next = `${location.pathname}${location.search}`;
  return <Navigate replace to={`/login?next=${encodeURIComponent(next)}`} />;
}
