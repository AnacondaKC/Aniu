export const memoryKeys = {
  all: ["memory-overview"] as const,
  overview: (
    activityPage: number,
    activityTaskId: number | undefined,
    activityOperation: string | undefined,
    memoryPage: number,
    memoryKeywords: string,
  ) =>
    [
      ...memoryKeys.all,
      "overview",
      activityPage,
      activityTaskId ?? null,
      activityOperation ?? "all",
      memoryPage,
      memoryKeywords,
    ] as const,
  dreams: (page: number) => [...memoryKeys.all, "dreams", page] as const,
  dreamDetail: (taskId: number) => [...memoryKeys.all, "dream-detail", taskId] as const,
};
