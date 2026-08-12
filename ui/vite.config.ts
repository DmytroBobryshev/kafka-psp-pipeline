import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// Dev-server proxy to api-gateway (M16), instead of straight to payment-api/realtime-gateway.
//
// Through M15 this proxied straight to payment-api (8085) and realtime-gateway (8090), because
// neither had CORS configured and api-gateway didn't exist yet (see ui/README.md "Why a proxy,
// not CORS" for that era's reasoning, kept in the README for the historical record). M16 adds
// api-gateway as the single REST entry point ADR-0004 describes - it now owns CORS, so the
// proxy is no longer strictly load-bearing for HOW the browser avoids a cross-origin request,
// only for keeping `pnpm dev` a same-origin, zero-config experience. Every `/api/*` request
// (both the payment-api paths and the realtime-gateway SSE path) now goes through ONE target:
// api-gateway on 8000, which itself routes to the six services (lb:// via Eureka - see
// services/api-gateway/README.md's route table).
//
// SSE still streams through this proxy exactly as it did through the old direct-to-8090 one:
// Vite's proxy (like api-gateway itself) is not a buffering reverse proxy - see
// services/api-gateway/README.md "SSE through the gateway" for the equivalent gateway-side
// argument, verified against a real payment.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});
