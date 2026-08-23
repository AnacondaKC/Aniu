import { useEffect, type ReactNode } from "react";

export type Direction = "ltr" | "rtl";

/**
 * Keeps the document direction explicit so Radix primitives and logical
 * Tailwind utilities mirror correctly when the application is embedded in an
 * RTL locale. Current product content is Simplified Chinese (LTR).
 */
export function DirectionProvider({
  children,
  dir = "ltr",
}: {
  children: ReactNode;
  dir?: Direction;
}) {
  useEffect(() => {
    document.documentElement.dir = dir;
  }, [dir]);

  return <>{children}</>;
}
