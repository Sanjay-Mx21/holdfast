# The web app: built to static files (next build, output: 'export'), served
# by nginx behind the edge.
FROM node:24.21.0-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
# 1 adds the development-inbox button to the sign-in page (local stack only).
ARG NEXT_PUBLIC_DEV_INBOX=0
ENV NEXT_PUBLIC_DEV_INBOX=$NEXT_PUBLIC_DEV_INBOX NEXT_TELEMETRY_DISABLED=1
RUN npm run build

FROM nginx:1.30.5-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/out /usr/share/nginx/html
