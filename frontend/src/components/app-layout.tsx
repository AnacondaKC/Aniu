import { useCallback, useState } from "react";
import { Outlet, useLocation } from "react-router-dom";
import { toast } from "sonner";

import { AppSidebar } from "@/components/app-sidebar";
import { Header } from "@/components/layout/header";
import { Main } from "@/components/layout/main";
import { SkipToMain } from "@/components/skip-to-main";
import { ThemeToggle } from "@/components/theme-toggle";
import { Button } from "@/components/ui/button";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { useAuth } from "@/features/auth/auth-context";
import { getPageMeta, settingsNavigationItems } from "@/lib/navigation";

function sidebarStartsOpen() {
  return typeof document === "undefined" || !document.cookie.includes("sidebar_state=false");
}

type MainLayoutOverride = {
  locationKey: string;
  fixed: boolean;
};

export function AppLayout() {
  const location = useLocation();
  const pageMeta = getPageMeta(location.pathname);
  const auth = useAuth();

  const isRuns = location.pathname === "/runs";
  const isMemories = location.pathname === "/memories";
  const isSettings = settingsNavigationItems.some(
    (item) => location.pathname === item.to || location.pathname.startsWith(`${item.to}/`),
  );
  const defaultFixedMain = isRuns || isMemories || isSettings;
  const scrollableFixedMain = location.pathname === "/" || isMemories;
  const [mainLayoutOverride, setMainLayoutOverride] = useState<MainLayoutOverride | null>(null);
  const setMainFixed = useCallback(
    (fixed: boolean) => {
      setMainLayoutOverride({ locationKey: location.key, fixed });
    },
    [location.key],
  );
  const usesFixedMain =
    mainLayoutOverride?.locationKey === location.key ? mainLayoutOverride.fixed : defaultFixedMain;

  return (
    <SidebarProvider defaultOpen={sidebarStartsOpen()}>
      <SkipToMain />
      <AppSidebar />
      <SidebarInset className={usesFixedMain ? "h-svh min-w-0 overflow-hidden" : "min-w-0"}>
        <Header fixed>
          <div className="me-auto flex min-w-0 flex-1 flex-col">
            <p className="truncate text-sm font-medium">{pageMeta.title}</p>
            {pageMeta.description ? (
              <p className="text-muted-foreground truncate text-xs">{pageMeta.description}</p>
            ) : null}
          </div>
          {auth.authenticated ? (
            <div className="text-muted-foreground me-2 flex items-center gap-2 text-xs">
              <span className="hidden sm:inline">{auth.username}</span>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => {
                  void auth.logout().then(() => toast.success("已退出登录"));
                }}
              >
                退出
              </Button>
            </div>
          ) : null}
          <ThemeToggle />
        </Header>
        <Main
          id="content"
          fixed={usesFixedMain}
          scrollable={usesFixedMain && scrollableFixedMain}
          className={usesFixedMain ? (isSettings ? "pb-4 lg:pb-6" : "pb-2 lg:pb-2") : undefined}
        >
          <Outlet context={{ setMainFixed }} />
        </Main>
      </SidebarInset>
    </SidebarProvider>
  );
}
