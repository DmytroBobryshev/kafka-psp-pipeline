import {
  Link,
  Outlet,
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
  redirect,
} from "@tanstack/react-router";
import { useIsFetching, useQueryClient } from "@tanstack/react-query";
import { TimelinePage } from "./pages/TimelinePage";
import { PaymentsPage } from "./pages/PaymentsPage";
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
const NAV_GROUPS = [
  {
    label: "Operations",
    items: [
      { to: "/payments", label: "Transactions" },
      { to: "/dashboard", label: "Dashboard" },
    ],
  },
  {
    label: "Simulation",
    items: [{ to: "/", label: "Simulator", exact: true }],
  },
  {
    label: "Configuration",
    items: [{ to: "/merchants", label: "Merchants" }],
  },
  {
    label: "Infrastructure",
    items: [
      { to: "/dlq", label: "DLQ" },
      { to: "/cluster", label: "Cluster" },
    ],
  },
] as const;

function RefreshButton() {
  // No background polling anywhere - data refreshes on navigation, on mutations, and here.
  const queryClient = useQueryClient();
  const fetching = useIsFetching();
  return (
    <button
      onClick={() => queryClient.invalidateQueries()}
      className="rounded-md border border-slate-400 bg-white px-3 py-1.5 text-sm font-medium text-slate-800 shadow-sm hover:bg-slate-200"
    >
      {fetching ? "Refreshing…" : "↻ Refresh"}
    </button>
  );
}

function Shell() {
  return (
    <div className="min-h-screen bg-slate-100 text-slate-900">
      <header className="border-b border-slate-300 bg-white">
        <div className="mx-auto flex max-w-[1500px] flex-wrap items-center justify-between gap-3 px-6 py-4">
          <div>
            <h1 className="text-xl font-semibold tracking-tight">Kafka PSP Pipeline</h1>
            <p className="mt-0.5 text-sm text-slate-600">
              Six windows into one event pipeline - every page is a Kafka concept, live.
            </p>
          </div>
          <nav className="flex flex-wrap items-center gap-3">
            {NAV_GROUPS.map((group) => (
              <div key={group.label} className="flex items-center gap-1 rounded-lg border border-slate-200 bg-slate-100 p-1">
                <span className="px-1.5 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
                  {group.label}
                </span>
                {group.items.map((item) => (
              <Link
                key={item.to}
                to={item.to}
                activeOptions={{ exact: "exact" in item && item.exact }}
                className="rounded-md px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-200 hover:text-slate-900 data-[status=active]:bg-slate-900 data-[status=active]:text-white data-[status=active]:hover:bg-slate-700 data-[status=active]:hover:text-white"
              >
                    {item.label}
                  </Link>
                ))}
              </div>
            ))}
            <RefreshButton />
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
  createRoute({
    getParentRoute: () => rootRoute,
    path: "/payments",
    component: PaymentsPage,
    validateSearch: (search: Record<string, unknown>) => ({
      merchantId: typeof search.merchantId === "string" ? search.merchantId : undefined,
      paymentId: typeof search.paymentId === "string" ? search.paymentId : undefined,
    }),
  }),
  createRoute({ getParentRoute: () => rootRoute, path: "/dashboard", component: DashboardPage }),
  createRoute({ getParentRoute: () => rootRoute, path: "/merchants", component: MerchantConfigPage }),
  createRoute({
    getParentRoute: () => rootRoute,
    path: "/refunds",
    // The standalone refunds page is merged into the transactions panel; old links land there.
    beforeLoad: ({ search }) => {
      throw redirect({ to: "/payments", search: search as never });
    },
    component: RefundTrackerPage,
    // Deep-linkable: the timeline's "refund →" action (and anything else) prefills the payment.
    validateSearch: (search: Record<string, unknown>) => ({
      paymentId: typeof search.paymentId === "string" ? search.paymentId : undefined,
    }),
  }),
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
