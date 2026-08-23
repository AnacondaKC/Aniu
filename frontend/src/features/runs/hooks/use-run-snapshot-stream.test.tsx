import { act, renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactNode } from "react";

import type { RunDetail, RunSummary } from "@/lib/api-types";
import { runKeys } from "@/features/runs/query-keys";
import { useRunSnapshotStream } from "./use-run-snapshot-stream";

vi.mock("@/lib/api", () => ({
  buildApiUrl: (path: string) => path,
}));

class MockEventSource {
  static instances: MockEventSource[] = [];
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  onmessage: ((message: MessageEvent) => void) | null = null;
  close = vi.fn();
  readonly url: string;
  readonly options: EventSourceInit | undefined;

  constructor(url: string, options?: EventSourceInit) {
    this.url = url;
    this.options = options;
    MockEventSource.instances.push(this);
  }
}

const runningDetail: RunDetail = {
  run_id: 12,
  task_id: 20260726001,
  status: "RUNNING",
  started_at: "2026-07-26T01:00:00Z",
  completed_at: null,
  current_state: "Run",
  trigger_source: "manual",
  total_tokens: 0,
  tool_calls_count: 0,
  thinking_count: 0,
  trade_count: 0,
  summary: null,
  summary_render_mode: "markdown",
  trace: {
    schema_version: 3,
    event_seq: 0,
    current_stage_id: "run:na",
    stages: [],
  },
};

const completedDetail: RunDetail = {
  ...runningDetail,
  status: "COMPLETED",
  completed_at: "2026-07-26T01:10:00Z",
  summary_render_mode: "html",
  trace: { ...runningDetail.trace, event_seq: 1, current_stage_id: null },
};

function createWrapper(client: QueryClient) {
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
}

function emit(source: MockEventSource | undefined, payload: object) {
  source?.onmessage?.({ data: JSON.stringify(payload) } as MessageEvent);
}

describe("useRunSnapshotStream", () => {
  beforeEach(() => {
    MockEventSource.instances = [];
    vi.clearAllMocks();
    vi.stubGlobal("EventSource", MockEventSource);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("writes an authoritative terminal checkpoint to the detail cache without treating it as a list", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const list: RunSummary[] = [{ ...runningDetail, trace: undefined } as RunSummary];
    client.setQueryData(runKeys.list(0), list);
    client.setQueryData(runKeys.detail(runningDetail.run_id), runningDetail);

    const { result } = renderHook(() => useRunSnapshotStream(runningDetail.run_id, runningDetail), {
      wrapper: createWrapper(client),
    });

    act(() => {
      emit(MockEventSource.instances[0], {
        kind: "checkpoint",
        stream_id: "stream-a",
        event_seq: 1,
        run_id: runningDetail.run_id,
        snapshot: completedDetail,
      });
    });

    await waitFor(() => expect(result.current.snapshot.status).toBe("COMPLETED"));
    expect(result.current.snapshot.summary_render_mode).toBe("html");
    expect(client.getQueryData<RunDetail>(runKeys.detail(runningDetail.run_id))?.status).toBe(
      "COMPLETED",
    );
    expect(client.getQueryData<RunSummary[]>(runKeys.list(0))?.[0]?.status).toBe("COMPLETED");
  });

  it("reconnects with a cursor on gaps and applies every replayed delta", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { result } = renderHook(() => useRunSnapshotStream(runningDetail.run_id, runningDetail), {
      wrapper: createWrapper(client),
    });
    const initial = MockEventSource.instances[0];

    act(() => {
      emit(initial, {
        kind: "checkpoint",
        stream_id: "stream-a",
        event_seq: 10,
        run_id: runningDetail.run_id,
        snapshot: runningDetail,
      });
      emit(initial, {
        kind: "trace_delta",
        stream_id: "stream-a",
        event_seq: 11,
        run_id: runningDetail.run_id,
        stage_id: "research-1",
        step_id: "thinking-1",
        channel: "content",
        delta: "baseline",
      });
      emit(initial, {
        kind: "trace_delta",
        stream_id: "stream-a",
        event_seq: 14,
        run_id: runningDetail.run_id,
        stage_id: "research-1",
        step_id: "thinking-1",
        channel: "content",
        delta: "gap-frame",
      });
    });

    const resumed = MockEventSource.instances[1];
    expect(initial?.close).toHaveBeenCalled();
    expect(resumed?.url).toContain("stream_id=stream-a");
    expect(resumed?.url).toContain("after_seq=11");

    act(() => {
      for (const [eventSeq, delta] of [
        [12, "-missed-a"],
        [13, "-missed-b"],
        [14, "-gap-frame"],
      ] as const) {
        emit(resumed, {
          kind: "trace_delta",
          stream_id: "stream-a",
          event_seq: eventSeq,
          run_id: runningDetail.run_id,
          stage_id: "research-1",
          step_id: "thinking-1",
          channel: "content",
          delta,
        });
      }
    });

    await waitFor(() =>
      expect(result.current.liveStepDeltaByStepId["research-1::thinking-1"]).toBe(
        "baseline-missed-a-missed-b-gap-frame",
      ),
    );
  });

  it("accepts a lower sequence checkpoint when the backend stream changes", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { result } = renderHook(() => useRunSnapshotStream(runningDetail.run_id, runningDetail), {
      wrapper: createWrapper(client),
    });
    const stream = MockEventSource.instances[0];

    act(() => {
      emit(stream, {
        kind: "checkpoint",
        stream_id: "old-process",
        event_seq: 500,
        run_id: runningDetail.run_id,
        snapshot: { ...runningDetail, trace: { ...runningDetail.trace, event_seq: 45 } },
      });
      stream?.onerror?.();
      emit(stream, {
        kind: "checkpoint",
        stream_id: "new-process",
        event_seq: 1,
        run_id: runningDetail.run_id,
        snapshot: {
          ...completedDetail,
          trace: { ...completedDetail.trace, event_seq: 46 },
        },
      });
    });

    await waitFor(() => expect(result.current.snapshot.status).toBe("COMPLETED"));
  });

  it("accepts a mid-run checkpoint whose hub sequence is far ahead of trace sequence", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const midRunDetail: RunDetail = {
      ...runningDetail,
      trace: { ...runningDetail.trace, event_seq: 45 },
    };
    const { result } = renderHook(() => useRunSnapshotStream(midRunDetail.run_id, midRunDetail), {
      wrapper: createWrapper(client),
    });
    const stream = MockEventSource.instances[0];

    act(() => {
      emit(stream, {
        kind: "checkpoint",
        stream_id: "stream-a",
        event_seq: 500,
        run_id: midRunDetail.run_id,
        snapshot: { ...midRunDetail, trace: { ...midRunDetail.trace, event_seq: 46 } },
      });
      emit(stream, {
        kind: "trace_delta",
        stream_id: "stream-a",
        event_seq: 501,
        run_id: midRunDetail.run_id,
        stage_id: "research-1",
        step_id: "result",
        channel: "text",
        delta: "streamed",
      });
    });

    await waitFor(() => expect(result.current.snapshot.trace.event_seq).toBe(46));
    await waitFor(() =>
      expect(result.current.liveStepDeltaByStepId["research-1::result"]).toBe("streamed"),
    );
  });

  it("ignores duplicate event sequences and reconnects with the latest cursor", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { result } = renderHook(() => useRunSnapshotStream(runningDetail.run_id, runningDetail), {
      wrapper: createWrapper(client),
    });
    const stream = MockEventSource.instances[0];

    act(() => {
      emit(stream, {
        kind: "checkpoint",
        stream_id: "stream-a",
        event_seq: 0,
        run_id: runningDetail.run_id,
        snapshot: runningDetail,
      });
      const frame = {
        kind: "trace_delta",
        stream_id: "stream-a",
        event_seq: 1,
        run_id: runningDetail.run_id,
        stage_id: "research-1",
        step_id: "thinking-1",
        channel: "content",
      };
      emit(stream, { ...frame, delta: "first" });
      emit(stream, { ...frame, delta: "duplicate" });
      stream?.onerror?.();
    });

    await waitFor(() =>
      expect(result.current.liveStepDeltaByStepId["research-1::thinking-1"]).toBe("first"),
    );
    expect(stream?.close).toHaveBeenCalled();
    await waitFor(() => expect(MockEventSource.instances).toHaveLength(2));
    expect(MockEventSource.instances[1]?.url).toContain("stream_id=stream-a&after_seq=1");
  });
});
