import { lazy, Suspense } from "react";
import { Navigate, RouterProvider, createBrowserRouter } from "react-router-dom";

import { AppLayout } from "./components/app-layout";
import { AuthProvider } from "./features/auth/auth-provider";
import { RequireAuth } from "./features/auth/require-auth";
import { RouteErrorPage, NotFoundPage } from "./components/route-error-page";
import { Spinner } from "./components/ui/spinner";

const DashboardPage = lazy(() =>
  import("./features/dashboard").then((module) => ({
    default: module.DashboardPage,
  })),
);
const RunsPage = lazy(() =>
  import("./features/runs").then((module) => ({ default: module.RunsPage })),
);
const MemoryOverviewPage = lazy(() =>
  import("./features/memories/memory-overview-page").then((module) => ({
    default: module.MemoryOverviewPage,
  })),
);
const SettingsLayout = lazy(() =>
  import("./features/settings/settings-layout").then((module) => ({
    default: module.SettingsLayout,
  })),
);
const MainSettingsLayout = lazy(() =>
  import("./features/settings/main-settings-layout").then((module) => ({
    default: module.MainSettingsLayout,
  })),
);
const StockApiSettingsPage = lazy(() =>
  import("./features/settings/stock-api-settings-page").then((module) => ({
    default: module.StockApiSettingsPage,
  })),
);
const StageSettingsPage = lazy(() =>
  import("./features/settings/stage-settings-page").then((module) => ({
    default: module.StageSettingsPage,
  })),
);
const LoginPage = lazy(() =>
  import("./features/auth/login-page").then((module) => ({
    default: module.LoginPage,
  })),
);

function PageFallback() {
  return (
    <div className="text-muted-foreground flex min-h-[240px] items-center justify-center">
      <Spinner />
    </div>
  );
}

function page(element: React.ReactNode) {
  return <Suspense fallback={<PageFallback />}>{element}</Suspense>;
}

const router = createBrowserRouter([
  {
    path: "/login",
    errorElement: <RouteErrorPage />,
    element: page(<LoginPage />),
  },
  {
    path: "/",
    element: <RequireAuth />,
    errorElement: <RouteErrorPage />,
    children: [
      {
        element: <AppLayout />,
        children: [
          {
            index: true,
            element: page(<DashboardPage />),
          },
          {
            path: "runs",
            element: page(<RunsPage />),
          },
          {
            path: "memories",
            element: page(<MemoryOverviewPage />),
          },
          {
            element: page(<SettingsLayout />),
            children: [
              {
                element: page(<MainSettingsLayout />),
                path: "settings",
              },
              {
                path: "stages",
                element: page(<StageSettingsPage />),
              },
              {
                // Preserve existing bookmarks after consolidating prompt settings
                // into the stage configuration surface.
                path: "prompts",
                element: <Navigate replace to="/stages" />,
              },
              {
                path: "stock-api",
                element: page(<StockApiSettingsPage />),
              },
            ],
          },
        ],
      },
    ],
  },
  {
    path: "*",
    element: <NotFoundPage />,
  },
]);

function App() {
  return (
    <AuthProvider>
      <RouterProvider router={router} />
    </AuthProvider>
  );
}

export default App;
