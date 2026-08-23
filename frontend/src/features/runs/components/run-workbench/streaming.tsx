import { Children, isValidElement, memo, useEffect, useRef, type ReactNode } from "react";
import rehypeRaw from "rehype-raw";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { cn } from "@/lib/utils";

const contentClassName =
  "font-sans text-[13px] leading-[1.6] tracking-[0.001em] text-foreground [&_a]:font-medium [&_a]:text-primary [&_a]:no-underline [&_a:hover]:underline [&_blockquote]:my-2 [&_blockquote]:max-w-[88ch] [&_blockquote]:border-s-2 [&_blockquote]:border-border [&_blockquote]:py-0.5 [&_blockquote]:ps-3 [&_blockquote]:pe-2 [&_blockquote]:text-muted-foreground [&_code]:rounded-sm [&_code]:bg-muted/55 [&_code]:px-1 [&_code]:py-0.5 [&_code]:font-sans [&_code]:text-[0.88em] [&_h1]:mb-2.5 [&_h1]:mt-0 [&_h1]:font-sans [&_h1]:text-[19px] [&_h1]:font-semibold [&_h1]:leading-6 [&_h1]:tracking-[-0.02em] [&_h1]:text-foreground [&_h2]:mb-2 [&_h2]:mt-5 [&_h2]:border-l-2 [&_h2]:border-primary/30 [&_h2]:ps-2.5 [&_h2]:font-sans [&_h2]:text-[16px] [&_h2]:font-semibold [&_h2]:leading-5 [&_h2]:tracking-[-0.015em] [&_h2]:text-foreground [&_h3]:mb-1.5 [&_h3]:mt-3.5 [&_h3]:font-sans [&_h3]:text-[14px] [&_h3]:font-medium [&_h3]:leading-5 [&_h3]:tracking-[0.005em] [&_h3]:text-foreground [&_h4]:mb-1 [&_h4]:mt-3 [&_h4]:text-[12px] [&_h4]:font-semibold [&_h4]:text-foreground [&_hr]:hidden [&_em]:font-semibold [&_em]:not-italic [&_li]:my-0.5 [&_li]:ml-4 [&_li]:pl-0.5 [&_li]:marker:text-muted-foreground [&_li]:list-disc [&_ol>li]:list-decimal [&_ol>li]:marker:font-medium [&_p]:mb-2 [&_p]:max-w-[88ch] [&_p:last-child]:mb-0 [&_pre]:my-2 [&_pre]:max-h-80 [&_pre]:overflow-auto [&_pre]:rounded-md [&_pre]:border [&_pre]:border-border/60 [&_pre]:bg-muted/35 [&_pre]:p-2 [&_pre]:text-[10.5px] [&_pre]:leading-4 [&_strong]:font-semibold [&_strong]:text-foreground [&_ul]:my-2 [&_ol]:my-2";

const processContentClassName =
  "!text-[11px] !leading-[1.55] !text-muted-foreground [&_h1]:!mb-2 [&_h1]:!text-[13px] [&_h1]:!font-medium [&_h1]:!leading-5 [&_h1]:!text-foreground [&_h2]:!mb-1.5 [&_h2]:!mt-3 [&_h2]:!border-border/35 [&_h2]:!ps-2 [&_h2]:!text-[12px] [&_h2]:!font-medium [&_h2]:!text-foreground [&_h3]:!mb-1 [&_h3]:!mt-2.5 [&_h3]:!text-[11px] [&_h3]:!font-medium [&_h3]:!text-foreground [&_p]:!mb-1.5 [&_strong]:!font-medium [&_strong]:!text-foreground";

const METADATA_LABELS = [
  "档案编号",
  "运行时间",
  "报告时间",
  "任务编号",
  "运行编号",
  "触发方式",
] as const;

function nodeText(children: ReactNode): string {
  return Children.toArray(children)
    .map((child) => {
      if (typeof child === "string" || typeof child === "number") {
        return String(child);
      }
      if (isValidElement<{ children?: ReactNode }>(child)) {
        return nodeText(child.props.children);
      }
      return "";
    })
    .join("")
    .trim();
}

function parseMetadataHeading(children: ReactNode) {
  const text = nodeText(children);

  for (const label of METADATA_LABELS) {
    const match = text.match(new RegExp(`^${label}\\s*[：:]\\s*(.+)$`));
    if (match) {
      return { label, value: (match[1] ?? "").trim() };
    }
  }

  return null;
}

function MetadataHeading({ label, value }: { label: string; value: string }) {
  const codeLike = label.includes("编号");

  return (
    <span className="me-4 mb-2 inline-flex items-baseline gap-1.5 text-[10.5px] leading-5">
      <span className="text-muted-foreground font-medium">{label}</span>
      <span className={cn("text-foreground", codeLike && "font-sans text-[10px] tabular-nums")}>
        {value}
      </span>
    </span>
  );
}

const markdownComponents = {
  h2: ({ children }: { children?: ReactNode }) => {
    const metadata = parseMetadataHeading(children);
    if (metadata) {
      return <MetadataHeading {...metadata} />;
    }
    return <h2>{children}</h2>;
  },
  table: ({ children }: { children?: ReactNode }) => (
    <div className="border-border/60 my-3 overflow-x-auto rounded-md border-y">
      <table className="w-full min-w-max border-collapse text-[11.5px] leading-[1.45] tabular-nums">
        {children}
      </table>
    </div>
  ),
  thead: ({ children }: { children?: ReactNode }) => (
    <thead className="bg-muted/30 text-foreground">{children}</thead>
  ),
  tbody: ({ children }: { children?: ReactNode }) => (
    <tbody className="[&_tr:last-child_td]:border-b-0">{children}</tbody>
  ),
  tr: ({ children }: { children?: ReactNode }) => (
    <tr className="hover:bg-muted/15 transition-colors">{children}</tr>
  ),
  th: ({ children }: { children?: ReactNode }) => (
    <th className="border-border/65 border-b px-2 py-1.5 text-start font-semibold whitespace-nowrap">
      {children}
    </th>
  ),
  td: ({ children }: { children?: ReactNode }) => (
    <td className="border-border/45 first:text-foreground border-b px-2 py-1.5 align-top first:font-medium">
      {children}
    </td>
  ),
};

function useStickToBottom<T extends HTMLElement>(dep: unknown) {
  const ref = useRef<T | null>(null);
  const isAtBottomRef = useRef(true);

  useEffect(() => {
    const element = ref.current;
    if (!element) {
      return;
    }

    const handleScroll = () => {
      const distance = element.scrollHeight - element.scrollTop - element.clientHeight;
      isAtBottomRef.current = distance < 24;
    };

    element.addEventListener("scroll", handleScroll, { passive: true });
    handleScroll();
    return () => element.removeEventListener("scroll", handleScroll);
  }, []);

  useEffect(() => {
    const element = ref.current;
    if (element && isAtBottomRef.current) {
      element.scrollTop = element.scrollHeight;
    }
  }, [dep]);

  return ref;
}

const ContentDocument = memo(function ContentDocument({
  content,
  variant,
  renderMode,
}: {
  content: string;
  variant: "report" | "process";
  renderMode: "markdown" | "html";
}) {
  return (
    <div
      className={cn(
        contentClassName,
        variant === "process" && processContentClassName,
        renderMode === "html" && "report-html",
      )}
    >
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={renderMode === "html" ? [rehypeRaw] : []}
        components={markdownComponents}
      >
        {content}
      </ReactMarkdown>
    </div>
  );
});

export function StreamingContent({
  content,
  streaming,
  /** When false, content flows in the parent scroller (no nested max-height scrollbar). */
  scrollable = true,
  className,
  variant = "report",
  renderMode = "markdown",
}: {
  content: string;
  streaming: boolean;
  scrollable?: boolean;
  className?: string;
  variant?: "report" | "process";
  renderMode?: "markdown" | "html";
}) {
  const ref = useStickToBottom<HTMLDivElement>(content);

  return (
    <div
      ref={scrollable ? ref : undefined}
      className={cn(scrollable && "max-h-[560px] overflow-auto pe-1", className)}
    >
      <ContentDocument content={content} variant={variant} renderMode={renderMode} />
      {streaming ? (
        <span className="animate-caret-blink text-primary ms-0.5 inline-block">▌</span>
      ) : null}
    </div>
  );
}
