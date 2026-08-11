import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// Dev-server proxy instead of backend CORS.
//
// payment-api (8085) and realtime-gateway (8090) both expose plain, un-CORS'd REST/SSE
// endpoints - the right choice for services that, per ADR-0004, are never meant to be called
// directly by a browser from a different origin in production (api-gateway, M16, will front
// them). Turning on CORS in two backend services purely so a *dev-only* Vite server can reach
// them would mean touching backend config for something that is not a backend concern at all.
//
// Instead, every browser request stays same-origin against the Vite dev server (5173), which
// forwards `/api/payments/*` to 8085 and `/api/realtime/*` to 8090 as a plain reverse proxy.
// The browser never sees a cross-origin request, so no CORS headers are ever needed - not now,
// not if a third backend service joins later. See ui/README.md "Why a proxy, not CORS".
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/api/payments": {
        target: "http://localhost:8085",
        changeOrigin: true,
      },
      "/api/realtime": {
        target: "http://localhost:8090",
        changeOrigin: true,
      },
    },
  },
});
