import { useMutation, useQueryClient } from "@tanstack/react-query";
import { PlayIcon } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { toast } from "sonner";

import { abortRun, startRun } from "@/lib/api";
import { getErrorMessage } from "@/lib/format";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import type { RunDetail } from "@/lib/api-types";
import { runKeys } from "@/features/runs/query-keys";

type RunStartButtonProps = {
  triggerLabel?: string;
  onStartRequested?: () => void;
  onStarted?: (run: RunDetail) => void;
  runningRunId?: number | null;
  isStopping?: boolean;
  onStopRequested?: (runId: number) => void;
  onStopFailed?: (runId: number) => void;
  disabled?: boolean;
};

export function RunStartButton({
  triggerLabel = "手动运行",
  onStartRequested,
  onStarted,
  runningRunId = null,
  isStopping = false,
  onStopRequested,
  onStopFailed,
  disabled = false,
}: RunStartButtonProps) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const mutation = useMutation({
    mutationFn: () => startRun(),
    onSuccess: (run) => {
      toast.success(`运行 ${run.task_id} 已启动`);
      void queryClient.invalidateQueries({ queryKey: runKeys.all });
      void queryClient.invalidateQueries({ queryKey: ["account"] });
      if (onStarted) {
        onStarted(run);
      } else {
        void navigate("/runs");
      }
    },
    onError: (error) => {
      toast.error(getErrorMessage(error));
    },
  });

  const stopMutation = useMutation({
    mutationFn: (runId: number) => abortRun(runId, "manual_stop"),
    onSuccess: () => {
      toast.success("已请求停止运行");
      void queryClient.invalidateQueries({ queryKey: runKeys.all });
      void queryClient.invalidateQueries({ queryKey: ["account"] });
    },
    onError: (error, runId) => {
      onStopFailed?.(runId);
      toast.error(getErrorMessage(error));
      void queryClient.invalidateQueries({ queryKey: runKeys.all });
    },
  });

  const isRunning = runningRunId !== null;
  const isBusy = mutation.isPending || stopMutation.isPending || isStopping;

  return (
    <Button
      onClick={() => {
        if (isRunning) {
          onStopRequested?.(runningRunId);
          stopMutation.mutate(runningRunId);
          return;
        }
        onStartRequested?.();
        mutation.mutate();
      }}
      disabled={isBusy || (!isRunning && disabled)}
    >
      {isRunning || mutation.isPending ? (
        <Spinner data-icon="inline-start" />
      ) : (
        <PlayIcon data-icon="inline-start" />
      )}
      {mutation.isPending
        ? "启动中…"
        : isStopping || stopMutation.isPending
          ? "停止中…"
          : isRunning
            ? "手动停止"
            : triggerLabel}
    </Button>
  );
}
