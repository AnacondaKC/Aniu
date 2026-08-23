import * as React from "react";
import { EyeIcon, EyeOffIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

type SecretInputProps = Omit<React.ComponentProps<"input">, "type">;

function SecretInput({ className, ...props }: SecretInputProps) {
  const [revealed, setRevealed] = React.useState(false);

  return (
    <div className="relative">
      <Input {...props} type={revealed ? "text" : "password"} className={cn("pe-10", className)} />
      <Button
        type="button"
        variant="ghost"
        size="icon-xs"
        className="text-muted-foreground hover:text-foreground absolute inset-e-2 top-1/2 -translate-y-1/2 hover:bg-transparent"
        onClick={() => setRevealed((current) => !current)}
        aria-label={revealed ? "隐藏密钥" : "显示密钥"}
        title={revealed ? "隐藏密钥" : "显示密钥"}
      >
        {revealed ? <EyeIcon className="size-3.5" /> : <EyeOffIcon className="size-3.5" />}
      </Button>
    </div>
  );
}

export { SecretInput };
