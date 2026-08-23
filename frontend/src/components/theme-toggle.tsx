import { MoonStarIcon, SunMediumIcon } from "lucide-react";
import { useTheme } from "next-themes";

import { Button } from "@/components/ui/button";

export function ThemeToggle() {
  const { resolvedTheme, setTheme } = useTheme();
  const isDark = resolvedTheme === "dark";

  return (
    <Button variant="outline" size="icon-sm" onClick={() => setTheme(isDark ? "light" : "dark")}>
      {isDark ? <SunMediumIcon /> : <MoonStarIcon />}
      <span className="sr-only">切换主题</span>
    </Button>
  );
}
