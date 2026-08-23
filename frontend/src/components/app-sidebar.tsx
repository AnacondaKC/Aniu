import { BotIcon } from "lucide-react";
import { NavLink, useLocation } from "react-router-dom";

import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/components/ui/sidebar";
import { navigationGroups } from "@/lib/navigation";

function isRouteActive(pathname: string, target: string) {
  const url = new URL(target, "https://aniu.local");

  if (url.pathname === "/") {
    return pathname === "/";
  }

  return pathname === url.pathname || pathname.startsWith(`${url.pathname}/`);
}

export function AppSidebar() {
  const location = useLocation();
  const { setOpenMobile } = useSidebar();

  return (
    <Sidebar collapsible="icon" variant="floating">
      <SidebarHeader className="min-h-16 gap-2 p-2">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              asChild
              size="lg"
              className="cursor-default group-data-[collapsible=icon]:justify-center! hover:bg-transparent hover:text-inherit focus-visible:ring-0 active:bg-transparent active:text-inherit data-[state=open]:bg-transparent data-[state=open]:text-inherit"
            >
              <div>
                <div className="bg-sidebar-primary text-sidebar-primary-foreground flex aspect-square size-8 shrink-0 items-center justify-center rounded-lg">
                  <BotIcon className="size-4" />
                </div>
                <div className="grid min-w-0 flex-1 text-start text-sm leading-tight group-data-[collapsible=icon]:hidden">
                  <span className="truncate font-semibold">Aniu</span>
                  <span className="truncate text-xs">交易智能体工作台</span>
                </div>
              </div>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent>
        {navigationGroups.map((group) => (
          <SidebarGroup key={group.title} className="p-2">
            <SidebarGroupLabel className="mb-1">{group.title}</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {group.items.map((item) => (
                  <SidebarMenuItem key={item.to}>
                    <SidebarMenuButton
                      asChild
                      isActive={isRouteActive(location.pathname, item.to)}
                      tooltip={item.title}
                      className="rounded-lg"
                    >
                      <NavLink to={item.to} onClick={() => setOpenMobile(false)}>
                        <item.icon />
                        <span>{item.title}</span>
                      </NavLink>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        ))}
      </SidebarContent>
    </Sidebar>
  );
}
