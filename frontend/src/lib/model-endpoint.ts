import type { ModelProtocol } from "@/lib/api-types";

function trimTrailingSlashes(value: string) {
  return value.replace(/\/+$/, "");
}

function stripKnownSuffix(pathname: string, suffixes: string[]) {
  const normalized = trimTrailingSlashes(pathname);

  for (const suffix of suffixes) {
    if (normalized === suffix) {
      return "";
    }

    if (normalized.endsWith(suffix)) {
      return normalized.slice(0, -suffix.length);
    }
  }

  return normalized;
}

function normalizeProtocolPath(protocol: ModelProtocol, pathname: string) {
  if (protocol === "claude_api") {
    const normalized = stripKnownSuffix(pathname, ["/messages"]);
    return normalized || "/v1";
  }

  const normalized = stripKnownSuffix(pathname, ["/chat/completions"]);
  return normalized || "/v1";
}

function buildAbsoluteUrl(input: string) {
  const trimmed = input.trim();
  if (!trimmed) {
    return null;
  }

  try {
    const url = new URL(trimmed);
    if (!/^https?:$/.test(url.protocol)) {
      return null;
    }
    return url;
  } catch {
    return null;
  }
}

export function normalizeModelBaseUrl(protocol: ModelProtocol, input: string) {
  const url = buildAbsoluteUrl(input);
  if (url === null) {
    return input.trim();
  }

  url.pathname = normalizeProtocolPath(protocol, url.pathname);
  url.search = "";
  url.hash = "";
  return trimTrailingSlashes(url.toString());
}
