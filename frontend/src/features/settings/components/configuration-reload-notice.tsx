import { Button } from "@/components/ui/button";

export function ConfigurationReloadNotice({
  visible,
  onReload,
}: {
  visible: boolean;
  onReload: () => void | Promise<void>;
}) {
  if (!visible) return null;

  return (
    <div
      role="alert"
      className="flex flex-col gap-3 rounded-md border border-amber-500/40 bg-amber-500/10 p-4 text-sm sm:flex-row sm:items-center sm:justify-between"
    >
      <span>本地草稿已保留。请重新加载服务端版本并手动合并后，再次保存。</span>
      <Button type="button" variant="outline" onClick={() => void onReload()}>
        重新加载服务端版本
      </Button>
    </div>
  );
}
