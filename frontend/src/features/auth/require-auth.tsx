import { Navigate, Outlet, useLocation } from "react-router-dom";

import { useAuth } from "@/features/auth/auth-context";

/** Gate protected app routes behind an authenticated browser session. */
export function RequireAuth() {
  const auth = useAuth();
  const location = useLocation();

  if (auth.authenticated) {
    return <Outlet />;
  }

  const next = `${location.pathname}${location.search}`;
  return <Navigate replace to={`/login?next=${encodeURIComponent(next)}`} />;
}
