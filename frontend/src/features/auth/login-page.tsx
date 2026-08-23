import { useState } from "react";
import { Navigate, useNavigate, useSearchParams } from "react-router-dom";
import { toast } from "sonner";

import { useAuth } from "@/features/auth/auth-context";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export function LoginPage() {
  const auth = useAuth();
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const nextPath = params.get("next") || "/";

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [pending, setPending] = useState(false);

  const mode = auth.identityInitialized ? ("login" as const) : ("setup" as const);

  if (auth.authenticated) {
    return <Navigate replace to={nextPath} />;
  }

  async function onSubmit(event: React.FormEvent) {
    event.preventDefault();
    setPending(true);
    try {
      if (mode === "setup") {
        await auth.setup(username.trim(), password);
        toast.success("本地身份已创建并登录");
      } else {
        await auth.login(username.trim(), password);
        toast.success("登录成功");
      }
      void navigate(nextPath, { replace: true });
    } catch (error) {
      const message = error instanceof Error ? error.message : "认证失败";
      toast.error(message);
    } finally {
      setPending(false);
    }
  }

  return (
    <main className="bg-background flex min-h-svh items-center justify-center p-4">
      <Card className="border-border/70 w-full max-w-md">
        <CardHeader>
          <CardTitle>{mode === "setup" ? "初始化本地身份" : "登录 Aniu"}</CardTitle>
          <CardDescription>
            {mode === "setup"
              ? "首次使用请创建本地身份。创建后所有写操作需要登录。"
              : "使用本地身份登录以管理运行与配置。"}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form
            className="space-y-4"
            onSubmit={(event) => {
              void onSubmit(event);
            }}
          >
            <div className="space-y-2">
              <Label htmlFor="username">用户名</Label>
              <Input
                id="username"
                autoComplete="username"
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                required
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="password">密码</Label>
              <Input
                id="password"
                type="password"
                autoComplete={mode === "setup" ? "new-password" : "current-password"}
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                minLength={mode === "setup" ? 8 : 1}
                required
              />
            </div>
            <Button className="w-full" type="submit" disabled={pending}>
              {pending ? "处理中…" : mode === "setup" ? "创建并登录" : "登录"}
            </Button>
          </form>
        </CardContent>
      </Card>
    </main>
  );
}
