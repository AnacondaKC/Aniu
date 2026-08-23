import { isRouteErrorResponse, useRouteError } from "react-router-dom";

import { Button } from "@/components/ui/button";

export function RouteErrorPage() {
  const error = useRouteError();
  const message = isRouteErrorResponse(error)
    ? `${error.status} ${error.statusText}`
    : error instanceof Error
      ? error.message
      : "页面发生了意外错误";

  return (
    <main className="flex min-h-svh items-center justify-center p-6">
      <div role="alert" className="max-w-md space-y-3 text-center">
        <h1 className="text-2xl font-semibold">页面无法加载</h1>
        <p className="text-muted-foreground text-sm">{message}</p>
        <Button type="button" onClick={() => window.location.reload()}>
          重新加载
        </Button>
      </div>
    </main>
  );
}

export function NotFoundPage() {
  return (
    <main className="flex min-h-svh items-center justify-center p-6">
      <div className="max-w-md space-y-3 text-center">
        <h1 className="text-2xl font-semibold">页面不存在</h1>
        <p className="text-muted-foreground text-sm">请检查地址，或返回工作台首页。</p>
        <Button type="button" onClick={() => window.location.assign("/")}>
          返回首页
        </Button>
      </div>
    </main>
  );
}
