import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { getErrorMessage } from "@/lib/format";

export function QueryLoadingState({ label = "正在加载…" }: { label?: string }) {
  return (
    <div
      role="status"
      aria-live="polite"
      className="text-muted-foreground flex min-h-40 items-center justify-center gap-2 text-sm"
    >
      <Spinner className="size-4" />
      {label}
    </div>
  );
}

export function QueryErrorState({
  error,
  onRetry,
  title = "加载失败",
}: {
  error: unknown;
  onRetry: () => void;
  title?: string;
}) {
  return (
    <div
      role="alert"
      className="border-destructive/30 bg-destructive/5 flex min-h-40 flex-col items-center justify-center gap-3 rounded-lg border p-6 text-center"
    >
      <div>
        <p className="text-destructive font-medium">{title}</p>
        <p className="text-muted-foreground mt-1 text-sm">{getErrorMessage(error)}</p>
      </div>
      <Button type="button" variant="outline" onClick={onRetry}>
        重新加载
      </Button>
    </div>
  );
}
