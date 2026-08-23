/**
 * Guards programming errors at configuration write boundaries.
 *
 * Revision zero is valid, but it must have arrived in a successful server
 * response; callers must not use this helper as a fallback generator.
 */
export function requireRevision(value: number | null | undefined, resource: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error(`无法保存 ${resource}：尚未加载服务端版本`);
  }
  return value;
}
