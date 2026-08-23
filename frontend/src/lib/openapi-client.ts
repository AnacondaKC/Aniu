import createClient, { createQuerySerializer } from "openapi-fetch";

import type { paths } from "@/generated/api-schema";
import { clearAuthSession, getCsrfToken, setAuthSession } from "@/lib/auth-session";

type QueryParamValue = string | number | boolean | null | undefined;

function resolveApiBaseUrl(): string {
  const env = import.meta.env as { VITE_API_BASE_URL?: unknown };
  const raw = env.VITE_API_BASE_URL;
  if (typeof raw !== "string" || raw.length === 0) {
    return "";
  }
  return raw.replace(/\/$/, "");
}

const API_BASE_URL = resolveApiBaseUrl();
const SAFE_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);

function clientBaseUrl() {
  if (/^https?:\/\//.test(API_BASE_URL)) {
    return API_BASE_URL;
  }

  const origin = typeof window === "undefined" ? "http://localhost" : window.location.origin;
  return `${origin}${API_BASE_URL}`;
}

class ApiError extends Error {
  status: number;
  payload: unknown;

  constructor(message: string, status: number, payload: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.payload = payload;
  }
}

export class ApiConflictError extends ApiError {
  resource: string;
  expectedRevision: number;
  actualRevision: number;
  requestId: string | null;

  constructor(
    message: string,
    payload: unknown,
    details: {
      resource: string;
      expectedRevision: number;
      actualRevision: number;
      requestId: string | null;
    },
  ) {
    super(message, 409, payload);
    this.name = "ApiConflictError";
    this.resource = details.resource;
    this.expectedRevision = details.expectedRevision;
    this.actualRevision = details.actualRevision;
    this.requestId = details.requestId;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function getConflictDetails(payload: unknown) {
  if (!isRecord(payload) || !isRecord(payload.error)) {
    return null;
  }
  const { error } = payload;
  if (
    error.code !== "ConfigurationConflictError" ||
    !isRecord(error.details) ||
    typeof error.details.resource !== "string" ||
    typeof error.details.expected_revision !== "number" ||
    typeof error.details.actual_revision !== "number"
  ) {
    return null;
  }
  return {
    resource: error.details.resource,
    expectedRevision: error.details.expected_revision,
    actualRevision: error.details.actual_revision,
    requestId: typeof error.request_id === "string" ? error.request_id : null,
  };
}

export function isApiConflictError(error: unknown): error is ApiConflictError {
  return error instanceof ApiConflictError;
}

export function buildApiUrl(path: string, params?: Record<string, QueryParamValue>) {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const query = new URLSearchParams();

  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value === undefined || value === null || value === "") {
        continue;
      }
      query.set(key, String(value));
    }
  }

  const suffix = query.size > 0 ? `${normalizedPath}?${query.toString()}` : normalizedPath;
  return API_BASE_URL ? `${API_BASE_URL}${suffix}` : suffix;
}

function formatDetail(raw: unknown): string {
  if (Array.isArray(raw)) {
    return raw
      .map((item: unknown) => {
        if (item && typeof item === "object" && "msg" in item) {
          return String(item.msg);
        }
        return String(item);
      })
      .join("; ");
  }
  return String(raw);
}

function extractErrorMessage(payload: unknown, response: Response) {
  if (typeof payload === "object" && payload) {
    if (
      "error" in payload &&
      payload.error &&
      typeof payload.error === "object" &&
      "message" in payload.error
    ) {
      return String(payload.error.message);
    }
    if ("detail" in payload) {
      return formatDetail(payload.detail);
    }
  }
  if (typeof payload === "string" && payload.trim()) {
    return payload;
  }
  return `${response.status} ${response.statusText}`;
}

function clearUnauthorizedSession() {
  clearAuthSession();
  setAuthSession({ identityInitialized: true });
  if (typeof window !== "undefined" && !window.location.pathname.startsWith("/login")) {
    const next = `${window.location.pathname}${window.location.search}`;
    window.location.assign(`/login?next=${encodeURIComponent(next)}`);
  }
}

type ApiResult<T> =
  | { data: T; error?: never; response: Response }
  | { data?: never; error: unknown; response: Response };

export function getResponseData<T>(result: ApiResult<T>): T {
  if (!result.response.ok || !("data" in result)) {
    const message = extractErrorMessage(result.error, result.response);
    const conflict = result.response.status === 409 ? getConflictDetails(result.error) : null;
    if (conflict) {
      throw new ApiConflictError(message, result.error, conflict);
    }
    throw new ApiError(message, result.response.status, result.error);
  }
  return result.data;
}

const serializeQuery = createQuerySerializer();

export const openapiClient = createClient<paths>({
  baseUrl: clientBaseUrl(),
  credentials: "include",
  cache: "no-store",
  headers: { Accept: "application/json" },
  querySerializer(query) {
    const sanitized = Object.fromEntries(
      Object.entries(query).filter(
        ([, value]) => value !== undefined && value !== null && value !== "",
      ),
    );
    return serializeQuery(sanitized);
  },
  // Resolve fetch at request time so tests and observability wrappers can replace it.
  fetch: (request) => globalThis.fetch(request),
});

openapiClient.use({
  onRequest({ request }) {
    const csrfToken = getCsrfToken();
    if (!csrfToken || SAFE_METHODS.has(request.method)) {
      return;
    }

    const headers = new Headers(request.headers);
    headers.set("X-CSRF-Token", csrfToken);
    return new Request(request, { headers });
  },
  onResponse({ response }) {
    if (response.status === 401) {
      clearUnauthorizedSession();
    }
  },
});
