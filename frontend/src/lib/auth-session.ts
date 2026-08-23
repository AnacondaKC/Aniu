/** In-memory auth session state shared by the API client and AuthProvider. */

export type AuthSessionState = {
  authenticated: boolean;
  identityInitialized: boolean;
  username: string | null;
  csrfToken: string | null;
};

type Listener = () => void;

let session: AuthSessionState = {
  authenticated: false,
  identityInitialized: false,
  username: null,
  csrfToken: null,
};

const listeners = new Set<Listener>();

export function getAuthSession(): AuthSessionState {
  return session;
}

export function setAuthSession(next: Partial<AuthSessionState>) {
  session = { ...session, ...next };
  listeners.forEach((listener) => listener());
}

export function clearAuthSession() {
  session = {
    authenticated: false,
    identityInitialized: session.identityInitialized,
    username: null,
    csrfToken: null,
  };
  listeners.forEach((listener) => listener());
}

export function subscribeAuthSession(listener: Listener) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function getCsrfToken() {
  return session.csrfToken;
}
