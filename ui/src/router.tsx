import {
  Link,
  Outlet,
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
  redirect,
} from "@tanstack/react-router";
import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
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
const NAV_ITEMS = [
  { to: "/payments", label: "Transactions" },
  { to: "/dashboard", label: "Dashboard" },
  { to: "/", label: "Simulator", exact: true },
  { to: "/merchants", label: "Merchants" },
  { to: "/dlq", label: "DLQ" },
  { to: "/cluster", label: "Cluster" },
] as const;

function RefreshButton() {
  // No background polling anywhere - data refreshes on navigation, on mutations, and here.
  // Busy state is LOCAL to the button's own click: navigation fetches must not dim it.
  const queryClient = useQueryClient();
  const [busy, setBusy] = useState(false);
  return (
    <button
      onClick={async () => {
        setBusy(true);
        try {
          await queryClient.invalidateQueries();
        } finally {
          setBusy(false);
        }
      }}
      className="rounded-md border border-slate-600 px-3 py-1.5 text-sm font-medium text-slate-200 hover:bg-slate-800 hover:text-white"
    >
      <span className={`transition-opacity duration-200 ${busy ? "opacity-50" : "opacity-100"}`}>
        ↻ Refresh
      </span>
    </button>
  );
}

function Shell() {
  return (
    <div className="min-h-screen bg-slate-100 text-slate-900">
      <header className="bg-slate-900 text-white shadow-md">
        <div className="mx-auto flex max-w-[1500px] flex-wrap items-center justify-between gap-x-6 gap-y-2 px-6 py-3">
          <div className="flex items-center gap-3">
            <span className="flex h-9 w-9 items-center justify-center rounded-lg bg-indigo-500 text-base font-bold">
              K
            </span>
            <div>
              <h1 className="text-base font-semibold leading-tight tracking-tight">Kafka PSP Pipeline</h1>
              <p className="text-xs text-slate-400">Six windows into one event pipeline — live.</p>
            </div>
          </div>
          <nav className="flex flex-wrap items-center gap-1">
            {NAV_ITEMS.map((item) => (
              <Link
                key={item.to}
                to={item.to}
                activeOptions={{ exact: "exact" in item && item.exact }}
                className="rounded-md px-3 py-1.5 text-sm font-medium text-slate-300 hover:bg-slate-800 hover:text-white data-[status=active]:bg-white data-[status=active]:text-slate-900 data-[status=active]:hover:bg-white data-[status=active]:hover:text-slate-900"
              >
                {item.label}
              </Link>
            ))}
            <span className="mx-2 h-5 w-px bg-slate-700" />
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
