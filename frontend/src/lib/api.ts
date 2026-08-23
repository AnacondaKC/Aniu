import type { components } from "@/generated/api-schema";
import type {
  RunDetail,
  RunTrace,
  MemoryItem,
  MemoryOverview,
  MemoryDream,
  MemoryDreamDetail,
  MemoryDreamList,
  CreateModelChannelPayload,
  CreateSchedulePayload,
  UpdateModelChannelPayload,
  UpdateSchedulePayload,
  UpdateSettingsPayload,
  StockApiLogToolSource,
  StockApiProvider,
} from "@/lib/api-types";
import {
  clearAuthSession,
  getAuthSession,
  setAuthSession,
  type AuthSessionState,
} from "@/lib/auth-session";
import { getResponseData, openapiClient } from "@/lib/openapi-client";

export { buildApiUrl } from "@/lib/openapi-client";

type Schemas = components["schemas"];
export type AuthSessionResponse = Schemas["SessionResponse"];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isNullableString(value: unknown): value is string | null | undefined {
  return value === undefined || value === null || isString(value);
}

function isTraceToolSource(value: unknown): value is "aggregate" | "mx" | "public" | "internal" {
  return value === "aggregate" || value === "mx" || value === "public" || value === "internal";
}

function isStockApiProvider(value: unknown): value is StockApiProvider {
  return value === "mx" || value === "eastmoney" || value === "tencent" || value === "sina";
}

function isTraceStockApiCall(value: unknown) {
  return (
    isRecord(value) &&
    isString(value.call_id) &&
    isStockApiProvider(value.provider) &&
    isString(value.interface_name) &&
    isString(value.interface_identifier) &&
    isString(value.operation_id) &&
    (value.response_characters === null || typeof value.response_characters === "number") &&
    isString(value.status) &&
    typeof value.duration_ms === "number" &&
    isNullableString(value.error_message)
  );
}

function isTraceToolCall(value: unknown) {
  return (
    value === null ||
    (isRecord(value) &&
      isString(value.call_id) &&
      isString(value.intent_line) &&
      isTraceToolSource(value.source) &&
      isString(value.tool_name) &&
      isString(value.display_name) &&
      (value.query_parameters === null || isString(value.query_parameters)) &&
      (value.model_content_characters === undefined ||
        value.model_content_characters === null ||
        typeof value.model_content_characters === "number") &&
      (value.stock_api_calls === undefined ||
        value.stock_api_calls === null ||
        (Array.isArray(value.stock_api_calls) && value.stock_api_calls.every(isTraceStockApiCall))))
  );
}

function isTraceStep(value: unknown) {
  return (
    isRecord(value) &&
    isString(value.step_id) &&
    isString(value.type) &&
    isString(value.title) &&
    isString(value.status) &&
    isNullableString(value.summary) &&
    isNullableString(value.content) &&
    isTraceToolCall(value.tool_call) &&
    isNullableString(value.started_at) &&
    isNullableString(value.ended_at)
  );
}

function isRunTrace(value: unknown): value is RunTrace {
  return (
    isRecord(value) &&
    typeof value.schema_version === "number" &&
    typeof value.event_seq === "number" &&
    isNullableString(value.current_stage_id) &&
    Array.isArray(value.stages) &&
    value.stages.every(
      (stage) =>
        isRecord(stage) &&
        isString(stage.stage_id) &&
        isString(stage.key) &&
        isNullableString(stage.started_at) &&
        isNullableString(stage.ended_at) &&
        Array.isArray(stage.steps) &&
        stage.steps.every(isTraceStep),
    )
  );
}

function requireRunDetail(payload: Schemas["RunDetailResponse"]): RunDetail {
  const { trace } = payload;
  if (!isRunTrace(trace)) {
    throw new Error("运行详情接口返回格式异常：trace 无效");
  }
  return { ...payload, trace };
}

function applySession(payload: AuthSessionResponse): AuthSessionState {
  // Preserve an already-known CSRF token when a session probe omits it, so a
  // partial response cannot silently disable authenticated writes.
  const current = getAuthSession();
  const next: AuthSessionState = {
    authenticated: payload.authenticated,
    identityInitialized: payload.identity_initialized,
    username: payload.username ?? null,
    csrfToken: payload.csrf_token ?? (payload.authenticated ? current.csrfToken : null),
  };
  setAuthSession(next);
  return next;
}

export async function fetchAuthSession() {
  const result = await openapiClient.GET("/api/aniu/auth/session");
  return applySession(getResponseData(result));
}

export async function login(token: string) {
  const result = await openapiClient.POST("/api/aniu/auth/login", {
    body: { token },
  });
  return applySession(getResponseData(result));
}

export async function setupIdentity(token: string) {
  const result = await openapiClient.POST("/api/aniu/auth/setup", {
    body: { token },
  });
  return applySession(getResponseData(result));
}

export async function logout() {
  const result = await openapiClient.POST("/api/aniu/auth/logout");
  getResponseData(result);
  clearAuthSession();
  setAuthSession({ identityInitialized: true });
}

export async function listRuns(limit = 50, offset = 0, startedDate?: string) {
  const result = await openapiClient.GET("/api/aniu/runs", {
    params: {
      query: {
        limit,
        offset,
        ...(startedDate === undefined ? {} : { started_date: startedDate }),
      },
    },
  });
  return getResponseData(result);
}

export async function getRunDetail(runId: number) {
  const result = await openapiClient.GET("/api/aniu/runs/{run_id}", {
    params: { path: { run_id: runId } },
  });
  return requireRunDetail(getResponseData(result));
}

export async function deleteRun(runId: number) {
  const result = await openapiClient.DELETE("/api/aniu/runs/{run_id}", {
    params: { path: { run_id: runId } },
  });
  return getResponseData(result);
}

export async function startRun() {
  const result = await openapiClient.POST("/api/aniu/runs/start");
  return requireRunDetail(getResponseData(result));
}

export async function abortRun(runId: number, reason = "user_requested") {
  const result = await openapiClient.POST("/api/aniu/runs/{run_id}/abort", {
    params: { path: { run_id: runId } },
    body: { reason },
  });
  return getResponseData(result);
}

export async function getAccountDashboard() {
  const result = await openapiClient.GET("/api/aniu/account/dashboard");
  return getResponseData(result);
}

export async function refreshAccountCache() {
  const result = await openapiClient.POST("/api/aniu/account/refresh");
  return getResponseData(result);
}

export async function getMarketIndices() {
  const result = await openapiClient.GET("/api/aniu/market/overview/indices");
  return getResponseData(result);
}

export async function getMarketDetails() {
  const result = await openapiClient.GET("/api/aniu/market/overview/details");
  return getResponseData(result);
}

export async function getSettings() {
  const result = await openapiClient.GET("/api/aniu/settings");
  return getResponseData(result);
}

export async function updateSettings(payload: UpdateSettingsPayload) {
  const result = await openapiClient.PUT("/api/aniu/settings", {
    body: payload,
  });
  return getResponseData(result);
}

export async function listModelChannels() {
  const result = await openapiClient.GET("/api/aniu/settings/channels");
  return getResponseData(result);
}

export async function deleteModelChannel(channelId: number, expectedRevision: number) {
  const result = await openapiClient.DELETE("/api/aniu/settings/channels/{channel_id}", {
    params: {
      path: { channel_id: channelId },
      query: { expected_revision: expectedRevision },
    },
  });
  return getResponseData(result);
}

export async function clearModelChannelApiKey(channelId: number, expectedRevision: number) {
  const result = await openapiClient.DELETE("/api/aniu/settings/channels/{channel_id}/api-key", {
    params: {
      path: { channel_id: channelId },
      query: { expected_revision: expectedRevision },
    },
  });
  return getResponseData(result);
}

export async function getMemoryOverview({
  activityLimit = 20,
  activityOffset = 0,
  activityTaskId,
  activityOperation,
  itemLimit = 20,
  itemOffset = 0,
  itemKeywords = "",
}: {
  activityLimit?: number;
  activityOffset?: number;
  activityTaskId?: number | null;
  activityOperation?: "read" | "create" | "update" | "delete" | null;
  itemLimit?: number;
  itemOffset?: number;
  itemKeywords?: string;
} = {}): Promise<MemoryOverview> {
  const result = await openapiClient.GET("/api/aniu/memories", {
    params: {
      query: {
        activity_limit: activityLimit,
        activity_offset: activityOffset,
        activity_task_id: activityTaskId ?? null,
        activity_operation: activityOperation ?? null,
        item_limit: itemLimit,
        item_offset: itemOffset,
        item_keywords: itemKeywords,
      },
    },
  });
  return getResponseData(result);
}

export async function listMemoryDreams({
  limit = 20,
  offset = 0,
}: { limit?: number; offset?: number } = {}): Promise<MemoryDreamList> {
  const result = await openapiClient.GET("/api/aniu/memory-dreams", {
    params: { query: { limit, offset } },
  });
  return getResponseData(result);
}

export async function runMemoryDream(): Promise<MemoryDream> {
  const result = await openapiClient.POST("/api/aniu/memory-dreams/run");
  return getResponseData(result);
}

export async function getMemoryDream(taskId: number): Promise<MemoryDreamDetail> {
  const result = await openapiClient.GET("/api/aniu/memory-dreams/{task_id}", {
    params: { path: { task_id: taskId } },
  });
  return getResponseData(result);
}

export async function deleteMemoryDream(taskId: number): Promise<void> {
  const result = await openapiClient.DELETE("/api/aniu/memory-dreams/{task_id}", {
    params: { path: { task_id: taskId } },
  });
  getResponseData(result);
}

export async function createMemory(payload: {
  content: string;
  reason: string;
}): Promise<MemoryItem> {
  const result = await openapiClient.POST("/api/aniu/memories", { body: payload });
  return getResponseData(result);
}

export async function updateMemory(
  memoryId: number,
  payload: { content: string; reason: string; expected_version: number },
): Promise<MemoryItem> {
  const result = await openapiClient.PUT("/api/aniu/memories/{memory_id}", {
    params: { path: { memory_id: memoryId } },
    body: payload,
  });
  return getResponseData(result);
}

export async function deleteMemory(memoryId: number, expectedVersion: number): Promise<void> {
  const result = await openapiClient.DELETE("/api/aniu/memories/{memory_id}", {
    params: { path: { memory_id: memoryId } },
    body: { expected_version: expectedVersion },
  });
  getResponseData(result);
}

export async function getStockApiSettings() {
  const result = await openapiClient.GET("/api/aniu/settings/stock-api");
  return getResponseData(result);
}

export async function listStockApiLogs(params?: {
  limit?: number;
  offset?: number;
  tool_source?: StockApiLogToolSource;
  tool_id?: string;
  status?: string;
}) {
  const result = await openapiClient.GET("/api/aniu/settings/stock-api/logs", {
    params: {
      query: {
        ...(params?.limit === undefined ? {} : { limit: params.limit }),
        ...(params?.offset === undefined ? {} : { offset: params.offset }),
        ...(params?.tool_source === undefined ? {} : { tool_source: params.tool_source }),
        ...(params?.tool_id === undefined ? {} : { tool_id: params.tool_id }),
        ...(params?.status === undefined ? {} : { status: params.status }),
      },
    },
  });
  return getResponseData(result);
}

export async function fetchModelCatalog(
  channelId: number,
  payload: Schemas["FetchModelCatalogRequest"],
) {
  const result = await openapiClient.POST("/api/aniu/settings/channels/{channel_id}/models/fetch", {
    params: { path: { channel_id: channelId } },
    body: payload,
  });
  return getResponseData(result);
}

export async function lookupModelsDevModel(modelName: string) {
  const result = await openapiClient.POST("/api/aniu/settings/models/models-dev/lookup", {
    body: { model_name: modelName },
  });
  return getResponseData(result);
}

export async function createModelChannel(payload: CreateModelChannelPayload) {
  const result = await openapiClient.POST("/api/aniu/settings/channels/with-models", {
    body: payload,
  });
  return getResponseData(result);
}

export async function updateModelChannel(channelId: number, payload: UpdateModelChannelPayload) {
  const result = await openapiClient.PUT("/api/aniu/settings/channels/{channel_id}/with-models", {
    params: { path: { channel_id: channelId } },
    body: payload,
  });
  return getResponseData(result);
}

export async function listSchedules() {
  const result = await openapiClient.GET("/api/aniu/schedules");
  return getResponseData(result);
}

export async function createSchedule(payload: CreateSchedulePayload) {
  const result = await openapiClient.POST("/api/aniu/schedules", {
    body: payload,
  });
  return getResponseData(result);
}

export async function updateSchedule(scheduleId: number, payload: UpdateSchedulePayload) {
  const result = await openapiClient.PUT("/api/aniu/schedules/{schedule_id}", {
    params: { path: { schedule_id: scheduleId } },
    body: payload,
  });
  return getResponseData(result);
}
