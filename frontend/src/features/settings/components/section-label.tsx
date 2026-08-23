import type { ReactNode } from "react";

/** Small muted heading with a leading icon, shared by settings form sections. */
export function SectionLabel({ icon, children }: { icon: ReactNode; children: ReactNode }) {
  return (
    <div className="text-muted-foreground flex items-center gap-1.5 text-xs font-medium">
      {icon}
      {children}
    </div>
  );
}
