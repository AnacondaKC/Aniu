export const runKeys = {
  all: ["runs"] as const,
  list: (page: number) => [...runKeys.all, "list", page] as const,
  active: () => [...runKeys.all, "active"] as const,
  detail: (id: number) => [...runKeys.all, "detail", id] as const,
};
