import { Outlet, useLocation } from "react-router-dom";

import { Separator } from "@/components/ui/separator";
import { settingsNavigationItems } from "@/lib/navigation";

export function SettingsLayout() {
  const location = useLocation();
  const activeItem =
    settingsNavigationItems.find(
      (item) => location.pathname === item.to || location.pathname.startsWith(`${item.to}/`),
    ) ?? settingsNavigationItems[0]!;

  return (
    <section className="flex min-h-0 flex-1 flex-col" aria-labelledby="settings-heading">
      <div className="flex-none space-y-0.5">
        <h1 id="settings-heading" className="text-2xl font-bold tracking-tight md:text-3xl">
          {activeItem.title}
        </h1>
        <p className="text-muted-foreground text-sm md:text-base">{activeItem.description}</p>
      </div>

      <Separator className="my-4 flex-none lg:my-6" />

      {/* Each settings page scrolls inside its own panel; this shell must not add a second scroll surface. */}
      <div className="flex min-h-0 min-w-0 flex-1 overflow-hidden p-1">
        <div className="relative min-h-0 min-w-0 flex-1 overflow-hidden">
          {/* Card slots keep 4px padding so focus rings clear the panel scrollport instead of clipping. */}
          <div className="-mx-1 h-full min-w-0 px-1.5 [&_[data-slot=card-content]]:px-1 [&_[data-slot=card-footer]]:px-1 [&_[data-slot=card-header]]:px-1 [&_[data-slot=card]]:rounded-none [&_[data-slot=card]]:border-0 [&_[data-slot=card]]:bg-transparent [&_[data-slot=card]]:bg-none [&_[data-slot=card]]:shadow-none">
            <Outlet />
          </div>
        </div>
      </div>
    </section>
  );
}
