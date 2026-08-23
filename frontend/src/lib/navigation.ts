import type { LucideIcon } from "lucide-react";
import {
  ChartCandlestickIcon,
  KeyRoundIcon,
  LayoutDashboardIcon,
  MessageSquareTextIcon,
  MoonStarIcon,
  NetworkIcon,
  Settings2Icon,
  TimerIcon,
  WorkflowIcon,
} from "lucide-react";

type NavigationItem = {
  title: string;
  description: string;
  to: string;
  icon: LucideIcon;
};

export type MainSettingsTabId = "mx" | "channels-models" | "trading-schedule";

type MainSettingsNavigationItem = Omit<NavigationItem, "to"> & {
  id: MainSettingsTabId;
};

type NavigationGroup = {
  title: string;
  items: NavigationItem[];
};

export const settingsNavigationItems: NavigationItem[] = [
  {
    title: "主要设置",
    description: "管理妙想密钥、模型渠道与自动运行计划",
    to: "/settings",
    icon: Settings2Icon,
  },
  {
    title: "阶段设置",
    description: "分阶段管理模型与提示词",
    to: "/stages",
    icon: MessageSquareTextIcon,
  },
  {
    title: "工具管理",
    description: "统一管理数据工具、系统工具与调用日志",
    to: "/stock-api",
    icon: ChartCandlestickIcon,
  },
];

/** Sections rendered inside the main settings surface. */
export const mainSettingsNavigationItems: MainSettingsNavigationItem[] = [
  {
    id: "mx",
    title: "妙想设置",
    description: "配置妙想 API 密钥，用于数据与模拟交易工具",
    icon: KeyRoundIcon,
  },
  {
    id: "channels-models",
    title: "渠道模型",
    description: "维护模型渠道和可供阶段选择的模型",
    icon: NetworkIcon,
  },
  {
    id: "trading-schedule",
    title: "交易任务",
    description: "交易时段内按固定间隔自动运行研究、决策、交易与总结",
    icon: TimerIcon,
  },
];

export const navigationGroups: NavigationGroup[] = [
  {
    title: "总览分析",
    items: [
      {
        title: "投资总览",
        description: "账户资产、持仓与委托",
        to: "/",
        icon: LayoutDashboardIcon,
      },
      {
        title: "任务运行",
        description: "任务运行总览",
        to: "/runs",
        icon: WorkflowIcon,
      },
      {
        title: "记忆梦境",
        description: "可验证交易经验的读取与演化",
        to: "/memories",
        icon: MoonStarIcon,
      },
    ],
  },
  {
    title: "项目设置",
    items: settingsNavigationItems,
  },
];

export function getPageMeta(pathname: string) {
  for (const group of navigationGroups) {
    for (const item of group.items) {
      const matched = matchNavigationItem(pathname, [item]);
      if (matched !== null) {
        return { title: matched.title, description: matched.description };
      }
    }
  }

  return {
    title: "Aniu",
    description: "",
  };
}

function matchNavigationItem(pathname: string, items: NavigationItem[]) {
  for (const item of items) {
    const target = new URL(item.to, "https://aniu.local");

    if (target.pathname === "/" && pathname === "/") {
      return item;
    }

    if (
      target.pathname !== "/" &&
      (pathname === target.pathname || pathname.startsWith(`${target.pathname}/`))
    ) {
      return item;
    }
  }
  return null;
}
