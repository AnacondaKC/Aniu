import { useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";

import { buildApiUrl } from "@/lib/api";
import type { RunDetail, RunSummary } from "@/lib/api-types";
import { runKeys } from "@/features/runs/query-keys";

type RunStreamMessage =
  | {
      schema_version?: number;
      kind: "checkpoint" | "snapshot";
      stream_id: string;
      event_seq: number;
      run_id?: number;
      snapshot: RunDetail;
    }
  | {
      schema_version?: number;
      kind: "trace_delta" | "trace_step_delta";
      stream_id: string;
      event_seq: number;
      run_id: number;
      stage_id: string;
      step_id: string;
      channel: string;
      delta: string;
    };

function buildTraceStepKey(stageId: string, stepId: string) {
  return `${stageId}::${stepId}`;
}

function pickRunSummary(detail: RunDetail): RunSummary {
  const { trace: _trace, ...rest } = detail;
  void _trace;
  return rest;
}

export function useRunSnapshotStream(runId: number | undefined, initialSnapshot: RunDetail) {
  const queryClient = useQueryClient();
  const [boundRunId, setBoundRunId] = useState(runId);
  const [baselineTraceSeq, setBaselineTraceSeq] = useState(initialSnapshot.trace.event_seq);
  const [snapshot, setSnapshot] = useState(initialSnapshot);
  const [liveStepDeltaByStepId, setLiveStepDeltaByStepId] = useState<Record<string, string>>({});
  const pendingStepDeltaRef = useRef<Record<string, string>>({});
  const flushHandleRef = useRef<number | null>(null);
  const traceSeqRef = useRef(initialSnapshot.trace.event_seq);
  const generationRef = useRef(0);

  if (boundRunId !== runId) {
    setBoundRunId(runId);
    setSnapshot(initialSnapshot);
    setLiveStepDeltaByStepId({});
    setBaselineTraceSeq(initialSnapshot.trace.event_seq);
  }

  useEffect(() => {
    if (!runId) {
      return;
    }

    const generation = ++generationRef.current;
    let eventSource: EventSource | null = null;
    let terminal = false;
    let cursor: { streamId: string; eventSeq: number } | null = null;
    let reconnectTimer: number | null = null;
    let reconnectDelay = 250;

    const isCurrent = () => generationRef.current === generation;
    const cancelReconnect = () => {
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
    };

    const flushStepDelta = () => {
      flushHandleRef.current = null;
      const pending = pendingStepDeltaRef.current;
      pendingStepDeltaRef.current = {};
      const entries = Object.entries(pending);
      if (entries.length === 0 || !isCurrent()) {
        return;
      }
      setLiveStepDeltaByStepId((previous) => {
        const next = { ...previous };
        entries.forEach(([stepId, delta]) => {
          next[stepId] = `${next[stepId] ?? ""}${delta}`;
        });
        return next;
      });
    };

    const updateRunLists = (summary: RunSummary) => {
      queryClient.setQueriesData<RunSummary[]>(
        {
          predicate: (query) =>
            query.queryKey[0] === runKeys.all[0] &&
            (query.queryKey[1] === "list" || query.queryKey[1] === "active") &&
            Array.isArray(query.state.data),
        },
        (old) => old?.map((item) => (item.run_id === summary.run_id ? summary : item)),
      );
    };

    const applyCheckpoint = (detail: RunDetail) => {
      if (detail.trace.event_seq < traceSeqRef.current) {
        return;
      }
      traceSeqRef.current = detail.trace.event_seq;
      setSnapshot(detail);
      setLiveStepDeltaByStepId({});
      pendingStepDeltaRef.current = {};
      queryClient.setQueryData(runKeys.detail(detail.run_id), detail);

      if (detail.status !== "RUNNING") {
        terminal = true;
        cancelReconnect();
        eventSource?.close();
        updateRunLists(pickRunSummary(detail));
        void queryClient.invalidateQueries({ queryKey: runKeys.detail(detail.run_id) });
      }
    };

    const streamUrl = (resume: typeof cursor) => {
      const base = buildApiUrl(`/api/aniu/sse/run/${runId}`);
      if (!resume) {
        return base;
      }
      const params = new URLSearchParams({
        stream_id: resume.streamId,
        after_seq: String(resume.eventSeq),
      });
      return `${base}?${params.toString()}`;
    };

    const appendDelta = (
      payload: Extract<RunStreamMessage, { kind: "trace_delta" | "trace_step_delta" }>,
    ) => {
      const key = buildTraceStepKey(payload.stage_id, payload.step_id);
      pendingStepDeltaRef.current[key] =
        `${pendingStepDeltaRef.current[key] ?? ""}${payload.delta}`;
      if (flushHandleRef.current === null) {
        flushHandleRef.current = window.requestAnimationFrame(flushStepDelta);
      }
    };

    const connect = (resume: typeof cursor = cursor) => {
      if (!isCurrent() || terminal) {
        return;
      }
      eventSource?.close();
      const source = new EventSource(streamUrl(resume), { withCredentials: true });
      eventSource = source;

      source.onopen = () => {
        reconnectDelay = 250;
      };
      source.onerror = () => {
        if (terminal || source !== eventSource) {
          source.close();
          return;
        }
        source.close();
        if (reconnectTimer !== null) {
          return;
        }
        const resume = cursor;
        const delay = reconnectDelay;
        reconnectDelay = Math.min(reconnectDelay * 2, 5_000);
        reconnectTimer = window.setTimeout(() => {
          reconnectTimer = null;
          connect(resume);
        }, delay);
      };
      source.onmessage = (message) => {
        if (!isCurrent() || terminal || source !== eventSource) {
          return;
        }
        try {
          const rawData = typeof message.data === "string" ? message.data : String(message.data);
          const payload = JSON.parse(rawData) as RunStreamMessage;
          if (
            (payload.run_id !== undefined && payload.run_id !== runId) ||
            typeof payload.stream_id !== "string" ||
            typeof payload.event_seq !== "number"
          ) {
            return;
          }

          if (payload.kind === "checkpoint" || payload.kind === "snapshot") {
            if (cursor?.streamId === payload.stream_id && payload.event_seq < cursor.eventSeq) {
              return;
            }
            cursor = { streamId: payload.stream_id, eventSeq: payload.event_seq };
            applyCheckpoint(payload.snapshot);
            return;
          }

          if (payload.kind !== "trace_delta" && payload.kind !== "trace_step_delta") {
            return;
          }
          if (
            cursor === null ||
            payload.stream_id !== cursor.streamId ||
            payload.event_seq > cursor.eventSeq + 1
          ) {
            connect(cursor);
            return;
          }
          if (payload.event_seq <= cursor.eventSeq) {
            return;
          }
          cursor = { streamId: payload.stream_id, eventSeq: payload.event_seq };
          appendDelta(payload);
        } catch {
          // Ignore malformed keep-alive or partial frames.
        }
      };
    };

    pendingStepDeltaRef.current = {};
    traceSeqRef.current = baselineTraceSeq;
    connect(null);

    return () => {
      generationRef.current += 1;
      if (flushHandleRef.current !== null) {
        window.cancelAnimationFrame(flushHandleRef.current);
        flushHandleRef.current = null;
      }
      cancelReconnect();
      eventSource?.close();
    };
  }, [baselineTraceSeq, runId, queryClient]);

  return { snapshot, liveStepDeltaByStepId };
}
