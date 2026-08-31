import {
  Link,
  Outlet,
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";
import { TimelinePage } from "./pages/TimelinePage";
import { DashboardPage } from "./pages/DashboardPage";
import { DlqConsolePage } from "./pages/DlqConsolePage";
import { MerchantConfigPage } from "./pages/MerchantConfigPage";
import { ClusterOpsPage } from "./pages/ClusterOpsPage";
import { RefundTrackerPage } from "./pages/RefundTrackerPage";

/**
 * Code-based TanStack Router (no file-based routing plugin - six static routes don't earn a
 * build step). Each route is one of M17's showcase pages; the shell holds the shared header
 * so per-page components own only their content, keeping page 1 byte-identical in behaviour
 * to the pre-router slice.
 */
const NAV = [
  { to: "/", label: "Timeline", exact: true },
  { to: "/dashboard", label: "Dashboard" },
  { to: "/merchants", label: "Merchant config" },
  { to: "/refunds", label: "Refunds" },
  { to: "/dlq", label: "DLQ" },
  { to: "/cluster", label: "Cluster" },
] as const;

function Shell() {
  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-3 px-6 py-4">
          <div>
            <h1 className="text-xl font-semibold tracking-tight">Kafka PSP Pipeline</h1>
            <p className="mt-0.5 text-sm text-slate-500">
              Six windows into one event pipeline - every page is a Kafka concept, live.
            </p>
          </div>
          <nav className="flex flex-wrap gap-1">
            {NAV.map((item) => (
              <Link
                key={item.to}
                to={item.to}
                activeOptions={{ exact: "exact" in item && item.exact }}
                className="rounded-md px-3 py-1.5 text-sm font-medium text-slate-600 hover:bg-slate-100"
                activeProps={{ className: "rounded-md px-3 py-1.5 text-sm font-medium bg-slate-900 text-white" }}
              >
                {item.label}
              </Link>
            ))}
          </nav>
        </div>
      </header>
      <Outlet />
    </div>
  );
}

const rootRoute = createRootRoute({ component: Shell });

const routes = [
  createRoute({ getParentRoute: () => rootRoute, path: "/", component: TimelinePage }),
  createRoute({ getParentRoute: () => rootRoute, path: "/dashboard", component: DashboardPage }),
  createRoute({ getParentRoute: () => rootRoute, path: "/merchants", component: MerchantConfigPage }),
  createRoute({ getParentRoute: () => rootRoute, path: "/refunds", component: RefundTrackerPage }),
  createRoute({ getParentRoute: () => rootRoute, path: "/dlq", component: DlqConsolePage }),
  createRoute({ getParentRoute: () => rootRoute, path: "/cluster", component: ClusterOpsPage }),
];

const router = createRouter({ routeTree: rootRoute.addChildren(routes) });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

export function AppRouter() {
  return <RouterProvider router={router} />;
}
