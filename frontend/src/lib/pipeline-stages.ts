/** Frontend mirror of the backend two-stage pipeline. */

type PipelineStageId = "Run" | "Summary";

type PipelineStageDef = {
  stageId: PipelineStageId;
  shortLabel: string;
};

const STAGE_PIPELINE: readonly PipelineStageDef[] = [
  { stageId: "Run", shortLabel: "执行" },
  { stageId: "Summary", shortLabel: "总结" },
] as const;

function stageById(stageId: string | null | undefined): PipelineStageDef | null {
  if (!stageId) return null;
  for (const stage of STAGE_PIPELINE) {
    if (stageId === stage.stageId || stageId.startsWith(stage.stageId)) return stage;
  }
  return null;
}

export function formatStageState(state: string | null | undefined): string {
  if (!state) return "--";
  if (state === "Completed" || state === "COMPLETED") return "已完成";
  if (state === "Failed" || state === "FAILED") return "失败";
  return stageById(state)?.shortLabel ?? state;
}
