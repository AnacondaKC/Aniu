import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import { createSchedule, listSchedules, updateSchedule } from "@/lib/api";
import { getErrorMessage } from "@/lib/format";
import { isApiConflictError, type ApiConflictError } from "@/lib/openapi-client";
import { QueryErrorState, QueryLoadingState } from "@/components/query-state";
import { ConfigurationConflictDialog } from "@/features/settings/components/configuration-conflict-dialog";
import { ConfigurationReloadNotice } from "@/features/settings/components/configuration-reload-notice";
import {
  ScheduleSettingsCards,
  type ScheduleSubmission,
} from "@/features/settings/components/schedule-settings-cards";

const SCHEDULES_QUERY_KEY = ["schedules"] as const;

/** Trading-window schedules: recurring pipeline runs during market hours. */
export function TradingSchedulesPage() {
  const queryClient = useQueryClient();
  const [conflict, setConflict] = useState<ApiConflictError | null>(null);
  const [needsReload, setNeedsReload] = useState(false);
  const [reloadKey, setReloadKey] = useState(0);
  const schedulesQuery = useQuery({
    queryKey: SCHEDULES_QUERY_KEY,
    queryFn: listSchedules,
  });
  const schedulesReady = schedulesQuery.isSuccess && schedulesQuery.data !== undefined;

  const scheduleMutation = useMutation({
    mutationFn: (submission: ScheduleSubmission) =>
      submission.scheduleId === undefined
        ? createSchedule(submission.payload)
        : updateSchedule(submission.scheduleId, submission.payload),
    onSuccess: (schedule) => {
      queryClient.setQueryData<Awaited<ReturnType<typeof listSchedules>>>(
        SCHEDULES_QUERY_KEY,
        (schedules) => {
          const current = schedules ?? [];
          const index = current.findIndex((item) => item.schedule_id === schedule.schedule_id);
          if (index === -1) return [...current, schedule];
          return current.map((item) =>
            item.schedule_id === schedule.schedule_id ? schedule : item,
          );
        },
      );
      toast.success("调度计划已保存");
    },
    onError: (error) => {
      if (isApiConflictError(error)) {
        setConflict(error);
        setNeedsReload(true);
        return;
      }
      toast.error(getErrorMessage(error));
    },
  });
  const reloadServerConfiguration = async () => {
    const result = await schedulesQuery.refetch();
    if (result.isError || result.data === undefined) return;
    setNeedsReload(false);
    setConflict(null);
    setReloadKey((value) => value + 1);
  };

  return (
    <section className="flex flex-col gap-4" aria-label="交易任务内容">
      {schedulesQuery.isPending ? (
        <QueryLoadingState label="正在加载调度计划…" />
      ) : schedulesQuery.isError && !schedulesQuery.data ? (
        <QueryErrorState
          error={schedulesQuery.error}
          title="调度计划加载失败"
          onRetry={() => void schedulesQuery.refetch()}
        />
      ) : (
        <div className="flex flex-col gap-6">
          {schedulesQuery.isError ? (
            <div
              role="alert"
              className="rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-800 dark:text-amber-200"
            >
              正在显示上次成功加载的调度计划；后台刷新失败。
            </div>
          ) : null}
          <ConfigurationReloadNotice
            visible={needsReload && conflict === null}
            onReload={reloadServerConfiguration}
          />
          <ScheduleSettingsCards
            key={reloadKey}
            schedules={schedulesQuery.data ?? []}
            savePending={scheduleMutation.isPending}
            writeDisabled={needsReload || !schedulesReady}
            onSubmit={(submission) => scheduleMutation.mutateAsync(submission)}
          />
        </div>
      )}

      <ConfigurationConflictDialog
        conflict={conflict}
        onKeepLocal={() => setConflict(null)}
        onReload={reloadServerConfiguration}
      />
    </section>
  );
}
