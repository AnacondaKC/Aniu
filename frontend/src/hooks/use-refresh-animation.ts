import { useCallback, useEffect, useRef, useState } from "react";

const DEFAULT_MINIMUM_DURATION_MS = 1_000;

type RefreshOperation = () => Promise<unknown>;

/** Keep a refresh affordance visible long enough for a click to feel acknowledged. */
export function useRefreshAnimation(minimumDurationMs = DEFAULT_MINIMUM_DURATION_MS) {
  const [isAnimating, setIsAnimating] = useState(false);
  const animationIdRef = useRef(0);
  const animationTimerRef = useRef<number | null>(null);

  useEffect(
    () => () => {
      animationIdRef.current += 1;
      if (animationTimerRef.current !== null) {
        window.clearTimeout(animationTimerRef.current);
      }
    },
    [],
  );

  const start = useCallback(
    (operation: RefreshOperation) => {
      const animationId = animationIdRef.current + 1;
      const startedAt = Date.now();
      animationIdRef.current = animationId;
      if (animationTimerRef.current !== null) {
        window.clearTimeout(animationTimerRef.current);
        animationTimerRef.current = null;
      }
      setIsAnimating(true);

      const stop = () => {
        if (animationIdRef.current !== animationId) return;
        animationTimerRef.current = null;
        setIsAnimating(false);
      };
      const finish = () => {
        const remainingMs = Math.max(0, minimumDurationMs - (Date.now() - startedAt));
        if (remainingMs === 0) stop();
        else animationTimerRef.current = window.setTimeout(stop, remainingMs);
      };

      void Promise.resolve().then(operation).then(finish, finish);
    },
    [minimumDurationMs],
  );

  return { isAnimating, start };
}
