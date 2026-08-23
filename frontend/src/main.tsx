import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import App from "./App";
import { DirectionProvider } from "./components/direction-provider";
import { Toaster } from "./components/ui/sonner";
import { TooltipProvider } from "./components/ui/tooltip";
import { getAuthSession, subscribeAuthSession } from "./lib/auth-session";
import "./index.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      refetchOnWindowFocus: false,
    },
  },
});

// Cached API data belongs to the authenticated browser session.  Clearing it
// on logout or a 401 prevents another session from briefly seeing stale data
// and prevents writes using an old configuration revision.
subscribeAuthSession(() => {
  if (!getAuthSession().authenticated) {
    queryClient.clear();
  }
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <DirectionProvider>
      <TooltipProvider>
        <QueryClientProvider client={queryClient}>
          <App />
          <Toaster richColors />
        </QueryClientProvider>
      </TooltipProvider>
    </DirectionProvider>
  </StrictMode>,
);
