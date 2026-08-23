import { useState, type FormEvent } from "react";
import { Navigate, useNavigate, useSearchParams } from "react-router-dom";
import { BotIcon, KeyRoundIcon } from "lucide-react";
import { toast } from "sonner";

import { useAuth } from "@/features/auth/auth-context";
import { setupIdentity } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { SecretInput } from "@/components/ui/secret-input";
import { Spinner } from "@/components/ui/spinner";

export function LoginPage() {
  const auth = useAuth();
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const nextPath = params.get("next") || "/";
  const firstRun = !auth.identityInitialized;

  const [token, setToken] = useState("");
  const [pending, setPending] = useState(false);

  if (auth.authenticated) {
    return <Navigate replace to={nextPath} />;
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalizedToken = token.trim();
    if (!normalizedToken) {
      return;
    }

    setPending(true);
    try {
      if (firstRun) {
        await setupIdentity(normalizedToken);
        toast.success("Token 已设置");
      } else {
        await auth.login(normalizedToken);
        toast.success("登录成功");
      }
      void navigate(nextPath, { replace: true });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Token 无效";
      toast.error(message);
    } finally {
      setPending(false);
    }
  }

  return (
    <main className="bg-muted/30 flex min-h-svh items-center justify-center p-4">
      <div className="w-full max-w-sm">
        <div className="mb-6 flex items-center justify-center gap-3">
          <div className="bg-primary text-primary-foreground flex size-10 shrink-0 items-center justify-center rounded-lg">
            <BotIcon className="size-5" aria-hidden="true" />
          </div>
          <p className="text-foreground text-3xl leading-none font-semibold">Aniu</p>
        </div>

        <Card className="border-border/70 gap-5 shadow-lg shadow-black/5">
          <CardHeader className="gap-2">
            <CardTitle>{firstRun ? "设置访问 Token" : "登录 Aniu"}</CardTitle>
            <CardDescription>
              {firstRun ? "首次使用请设置访问 Token。" : "输入访问 Token 继续使用工作台。"}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form
              className="space-y-5"
              onSubmit={(event) => {
                void onSubmit(event);
              }}
            >
              <div className="space-y-2">
                <Label htmlFor="token">访问 Token</Label>
                <SecretInput
                  id="token"
                  name="token"
                  placeholder="输入访问 Token"
                  autoComplete={firstRun ? "new-password" : "current-password"}
                  spellCheck={false}
                  value={token}
                  onChange={(event) => setToken(event.target.value)}
                  required
                  minLength={8}
                  disabled={pending}
                />
              </div>
              <Button className="w-full" type="submit" disabled={pending} aria-busy={pending}>
                {pending ? (
                  <>
                    <Spinner aria-hidden="true" />
                    验证中
                  </>
                ) : (
                  <>
                    <KeyRoundIcon aria-hidden="true" />
                    {firstRun ? "保存并登录" : "登录"}
                  </>
                )}
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>
    </main>
  );
}
