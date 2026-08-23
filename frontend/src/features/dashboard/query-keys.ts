export const accountKeys = {
  all: ["account"] as const,
  dashboard: () => [...accountKeys.all, "dashboard"] as const,
};
