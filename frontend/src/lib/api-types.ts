/**
 * App-facing API types bridged from the generated OpenAPI schema, with
 * UI-side refinements for fields OpenAPI currently types loosely.
 */
import type { components } from "@/generated/api-schema";

type Schemas = components["schemas"];

export type ModelProtocol = Schemas["ModelProtocol"];
export type RunSummary = Schemas["RunSummaryResponse"];
export type ModelProfile = Schemas["ModelProfileResponse"];
export type ModelCatalogItem = Schemas["ModelCatalogItemResponse"];
export type ModelsDevModel = Schemas["ModelsDevModelResponse"];
export type StrategySchedule = Schemas["StrategyScheduleResponse"];
export type StageSettings = Schemas["StageSettingsResponse"];
export type UpdateSettingsPayload = Omit<Schemas["UpdateSettingsRequest"], "stage_settings"> & {
  stage_settings?: StageSettings[];
};
export type StockApiSettings = Schemas["StockApiSettingsResponse"];
export type StockApiLogToolSource = Schemas["StockApiCallLogResponse"]["tool_source"];
export type StockApiPublicProvider = Schemas["PublicStockToolResponse"]["providers"][number];
export type StockApiProvider = Schemas["TraceStockApiCallResponse"]["provider"];
export type MarketOverview = Schemas["MarketOverviewResponse"];
export type CreateSchedulePayload = Schemas["CreateScheduleRequest"];
export type UpdateSchedulePayload = Schemas["UpdateScheduleRequest"];
export type CreateModelChannelPayload = Schemas["CreateModelChannelWithModelsRequest"];
export type UpdateModelChannelPayload = Schemas["UpdateModelChannelWithModelsRequest"];
export type ModelCatalogFetchPayload = Schemas["FetchModelCatalogRequest"];
export type SelectedModelPayload = Schemas["SelectedModelRequest"];
export type ModelProfilePayload = Omit<
  Schemas["CreateModelChannelWithModelsRequest"],
  "selected_models"
>;

export type TraceStageKey = "run" | "summary";

export type TraceToolSource = "aggregate" | "mx" | "public" | "internal";

export interface TraceStockApiCall {
  call_id: string;
  provider: StockApiProvider;
  interface_name: string;
  interface_identifier: string;
  operation_id: string;
  parameters: unknown;
  response_characters: number | null;
  status: string;
  duration_ms: number;
  error_message: string | null;
}

interface TraceToolCall {
  call_id: string;
  intent_line: string;
  source: TraceToolSource;
  tool_name: string;
  display_name: string;
  query_parameters: string | null;
  model_content_characters?: number | null;
  stock_api_calls?: TraceStockApiCall[] | null;
}

/** Runtime/UI projection of a trace step (validated client-side). */
export interface TraceStep {
  step_id: string;
  type: "thinking" | "tool" | "result" | "status";
  title: string;
  status: "pending" | "running" | "completed" | "failed" | "blocked";
  summary: string | null;
  content: string | null;
  tool_call: TraceToolCall | null;
  started_at: string | null;
  ended_at: string | null;
}

/** Runtime/UI projection of a trace stage (validated client-side). */
export interface TraceStage {
  stage_id: string;
  key: TraceStageKey;
  status: "pending" | "running" | "completed" | "degraded" | "failed" | "skipped";
  started_at: string | null;
  ended_at: string | null;
  steps: TraceStep[];
}

/** Runtime/UI projection of a run trace (validated client-side). */
export interface RunTrace {
  schema_version: number;
  event_seq: number;
  current_stage_id: string | null;
  stages: TraceStage[];
}

export type RunDetail = Omit<Schemas["RunDetailResponse"], "trace"> & {
  trace: RunTrace;
};

export interface MemoryActivity {
  id: number;
  operation: "read" | "create" | "update" | "delete";
  memory_id: number | null;
  content: string;
  result_count: number | null;
  task_id: number;
  created_at: string;
}

export interface MemoryItem {
  id: number;
  content: string;
  reason: string;
  created_task_id: number;
  updated_task_id: number;
  version: number;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
}

export interface MemoryOverview {
  activities: MemoryActivity[];
  activity_total: number;
  items: MemoryItem[];
  item_total: number;
  item_match_total: number;
  generated_at: string;
}

export interface MemoryDream {
  task_id: number;
  target_date: string;
  status: "pending" | "running" | "completed" | "failed";
  result: string | null;
  failure_reason: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface MemoryDreamList {
  items: MemoryDream[];
  total: number;
  latest: MemoryDream | null;
}

export interface MemoryDreamDetail {
  dream: MemoryDream;
  activities: MemoryActivity[];
  activity_total: number;
}
