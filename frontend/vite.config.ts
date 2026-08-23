import path from "path";

import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

function splitCsv(value: string | undefined) {
  return (value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

const allowedHosts = Array.from(
  new Set([
    "localhost",
    "127.0.0.1",
    "ceshi.iniko.cc",
    ...splitCsv(process.env.VITE_ALLOWED_HOSTS),
  ]),
);

const hmrProtocol = process.env.VITE_HMR_PROTOCOL;
const hmrClientPort = Number.parseInt(process.env.VITE_HMR_CLIENT_PORT ?? "", 10);

const hmr =
  process.env.VITE_HMR_HOST || hmrProtocol || Number.isFinite(hmrClientPort)
    ? {
        ...(process.env.VITE_HMR_HOST ? { host: process.env.VITE_HMR_HOST } : {}),
        ...(hmrProtocol === "ws" || hmrProtocol === "wss" ? { protocol: hmrProtocol } : {}),
        ...(Number.isFinite(hmrClientPort) ? { clientPort: hmrClientPort } : {}),
      }
    : undefined;

const reactRefreshPreamble = {
  name: "aniu:react-refresh-preamble",
  apply: "serve" as const,
  enforce: "pre" as const,
  transformIndexHtml: {
    order: "pre" as const,
    handler() {
      return [
        {
          tag: "script",
          attrs: { type: "module" },
          children: `import { injectIntoGlobalHook } from "/@react-refresh";
injectIntoGlobalHook(window);
window.$RefreshReg$ = () => {};
window.$RefreshSig$ = () => (type) => type;`,
        },
      ];
    },
  },
};

// https://vite.dev/config/
export default defineConfig({
  plugins: [reactRefreshPreamble, react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    allowedHosts,
    hmr,
    proxy: {
      "/api": {
        target: process.env.VITE_BACKEND_PROXY ?? "http://127.0.0.1:8000",
        changeOrigin: true,
      },
      "/health": {
        target: process.env.VITE_BACKEND_PROXY ?? "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
});
