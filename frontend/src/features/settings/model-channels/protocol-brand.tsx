import { AsteriskIcon, MessageSquareTextIcon } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import type { ModelProtocol } from "@/lib/api-types";
import { cn } from "@/lib/utils";

type ProtocolBrand = {
  icon: LucideIcon;
  /** Brand tint on the icon itself; the explicit text-* class also opts out of the muted svg override in Select. */
  iconClassName: string;
};

const PROTOCOL_BRANDS: Record<ModelProtocol, ProtocolBrand> = {
  openai_chat_completions: {
    icon: MessageSquareTextIcon,
    iconClassName: "text-emerald-600 dark:text-emerald-400",
  },
  claude_api: {
    icon: AsteriskIcon,
    iconClassName: "text-orange-600 dark:text-orange-400",
  },
};

function protocolBrand(protocol: ModelProtocol): ProtocolBrand {
  return PROTOCOL_BRANDS[protocol] ?? PROTOCOL_BRANDS.openai_chat_completions;
}

/** Plain protocol-tinted icon that keys the channel visually to its provider. */
export function ProtocolBrandIcon({
  protocol,
  size = "md",
  className,
}: {
  protocol: ModelProtocol;
  size?: "sm" | "md";
  className?: string;
}) {
  const brand = protocolBrand(protocol);
  const Icon = brand.icon;

  return (
    <Icon
      aria-hidden="true"
      className={cn(
        "shrink-0",
        size === "md" ? "size-5" : "size-4",
        brand.iconClassName,
        className,
      )}
    />
  );
}
