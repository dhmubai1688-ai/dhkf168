FROM node:22-bookworm-slim AS frontend
WORKDIR /src/frontend
COPY frontend.tar.gz /tmp/frontend.tar.gz
RUN tar -xzf /tmp/frontend.tar.gz -C /src/frontend && rm /tmp/frontend.tar.gz
RUN npm ci --no-audit --no-fund
RUN npm run build

FROM golang:1.26-bookworm AS backend
WORKDIR /src/backend
COPY backend.tar.gz shared.tar.gz /tmp/
RUN mkdir -p /src/shared && tar -xzf /tmp/backend.tar.gz -C /src/backend && tar -xzf /tmp/shared.tar.gz -C /src/shared && rm /tmp/backend.tar.gz /tmp/shared.tar.gz
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /app/tg-attendance .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata && rm -rf /var/lib/apt/lists/* && useradd --system --uid 10001 app
WORKDIR /app
COPY --from=backend /app/tg-attendance ./tg-attendance
COPY --from=frontend /src/frontend/dist ./public
ENV ENV=production FRONTEND_DIST=/app/public TZ=Asia/Shanghai GOMEMLIMIT=256MiB GOGC=70
USER app
EXPOSE 8080
CMD ["/app/tg-attendance"]
