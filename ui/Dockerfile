# M17: static React bundle behind nginx. Multi-stage so the image ships only dist/ + nginx -
# node, pnpm and node_modules never leave the builder. Built by infra/k8s/scripts/build-images.sh
# as psp/ui:<tag> and loaded into kind like every other image (no registry - see that script).
FROM node:22-alpine AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile
COPY index.html tsconfig.json tsconfig.app.json tsconfig.node.json vite.config.ts ./
COPY src ./src
RUN pnpm build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
