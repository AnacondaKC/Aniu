import { useCallback, useState } from "react";
import { Outlet, useLocation } from "react-router-dom";
import { LogOutIcon } from "lucide-react";
import { toast } from "sonner";

import { AppSidebar } from "@/components/app-sidebar";
import { Header } from "@/components/layout/header";
import { Main } from "@/components/layout/main";
import { SkipToMain } from "@/components/skip-to-main";
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
          <div className="text-muted-foreground me-2 flex items-center gap-1">
            <Button asChild variant="ghost" size="icon">
              <a
                href="https://github.com/AnacondaKC/Aniu"
                target="_blank"
                rel="noopener noreferrer"
                aria-label="打开 Aniu GitHub 仓库"
                title="打开 Aniu GitHub 仓库"
              >
                <svg viewBox="0 0 24 24" className="size-4 fill-current" aria-hidden="true">
                  <path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.084-.729.084-.729 1.205.084 1.838 1.237 1.838 1.237 1.07 1.835 2.809 1.305 3.495.998.108-.776.418-1.305.762-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12" />
                </svg>
              </a>
            </Button>
            {auth.authenticated ? (
              <>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  aria-label="退出登录"
                  title="退出登录"
                  onClick={() => {
                    void auth.logout().then(() => toast.success("已退出登录"));
                  }}
                >
                  <LogOutIcon aria-hidden="true" />
                </Button>
              </>
            ) : null}
          </div>
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
