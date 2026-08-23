import type { ApiConflictError } from "@/lib/openapi-client";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";

type ConfigurationConflictDialogProps = {
  conflict: ApiConflictError | null;
  onKeepLocal: () => void;
  onReload: () => void | Promise<void>;
};

export function ConfigurationConflictDialog({
  conflict,
  onKeepLocal,
  onReload,
}: ConfigurationConflictDialogProps) {
  return (
    <AlertDialog open={conflict !== null}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>配置已被其他会话修改</AlertDialogTitle>
          <AlertDialogDescription>
            当前草稿基于版本 {conflict?.expectedRevision ?? "--"}，服务端已更新到版本{" "}
            {conflict?.actualRevision ?? "--"}。为避免覆盖其他会话的修改，系统没有自动重试保存。
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel onClick={onKeepLocal}>保留本地草稿</AlertDialogCancel>
          <AlertDialogAction onClick={() => void onReload()}>重新加载服务端版本</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
