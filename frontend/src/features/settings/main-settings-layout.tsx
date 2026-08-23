import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { MxSettingsPage } from "@/features/settings/components/mx-settings-section";
import { ModelChannelsSettingsPage } from "@/features/settings/model-channels-settings-page";
import { TradingSchedulesPage } from "@/features/settings/schedules-page";
import { mainSettingsNavigationItems, type MainSettingsTabId } from "@/lib/navigation";
import { cn } from "@/lib/utils";

export function MainSettingsLayout() {
  const [activeTab, setActiveTab] = useState<MainSettingsTabId>("mx");
  const activeItem = mainSettingsNavigationItems.find((item) => item.id === activeTab)!;
  const ActiveIcon = activeItem.icon;
  const activeContent = {
    mx: <MxSettingsPage />,
    "channels-models": <ModelChannelsSettingsPage />,
    "trading-schedule": <TradingSchedulesPage />,
  }[activeTab];

  return (
    <section className="h-full min-h-0 overflow-hidden" aria-label="主要设置内容">
      <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[11rem_minmax(0,1fr)] xl:gap-12">
        <aside className="top-0 h-fit xl:sticky">
          <nav className="space-y-1 p-1" aria-label="主要设置导航" role="tablist">
            {mainSettingsNavigationItems.map((item) => {
              const active = item.id === activeTab;
              const Icon = item.icon;

              return (
                <Button
                  key={item.id}
                  id={`main-settings-tab-${item.id}`}
                  type="button"
                  role="tab"
                  aria-selected={active}
                  aria-controls="main-settings-panel"
                  variant="ghost"
                  className={cn(
                    "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground active:bg-sidebar-accent active:text-sidebar-accent-foreground dark:hover:bg-sidebar-accent h-9 w-full justify-start gap-2 rounded-md px-3",
                    active && "bg-sidebar-accent text-sidebar-accent-foreground",
                  )}
                  onClick={() => setActiveTab(item.id)}
                >
                  <Icon className="size-4 shrink-0" />
                  <span>{item.title}</span>
                </Button>
              );
            })}
          </nav>
        </aside>

        <Card
          id="main-settings-panel"
          role="tabpanel"
          aria-labelledby={`main-settings-tab-${activeTab}`}
          className="h-full min-h-0 gap-2 overflow-hidden py-4"
        >
          <CardHeader className="bg-background flex-none !gap-1.5 border-b !pb-1">
            <div className="flex items-start gap-3">
              <div key={activeTab} className="text-primary pt-0.5">
                <ActiveIcon className="size-5" />
              </div>
              <div key={`${activeTab}-text`} className="min-w-0">
                <CardTitle>{activeItem.title}</CardTitle>
                <CardDescription className="mt-1">{activeItem.description}</CardDescription>
              </div>
            </div>
          </CardHeader>
          <CardContent className="min-h-0 flex-1 overflow-y-auto pt-2 pb-6">
            <div key={activeTab}>{activeContent}</div>
          </CardContent>
        </Card>
      </div>
    </section>
  );
}
