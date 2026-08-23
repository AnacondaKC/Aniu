import { cn } from "@/lib/utils";

type MainProps = React.HTMLAttributes<HTMLElement> & {
  fixed?: boolean;
  scrollable?: boolean;
  ref?: React.Ref<HTMLElement>;
};

export function Main({ fixed, scrollable, className, ...props }: MainProps) {
  return (
    <main
      data-layout={fixed ? "fixed" : "auto"}
      className={cn(
        "mx-auto w-full max-w-7xl px-3 pt-6 pb-16 sm:px-5 lg:px-6 lg:pb-24",
        fixed &&
          (scrollable
            ? "flex min-h-0 grow flex-col overflow-x-hidden overflow-y-auto"
            : "flex grow flex-col overflow-hidden"),
        className,
      )}
      {...props}
    />
  );
}
