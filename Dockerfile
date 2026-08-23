# syntax=docker/dockerfile:1.7

FROM node:22-bookworm-slim@sha256:d649c27dae7ba0137b3cef5dd75baa422c08dc3d9e3fc0c23dfb172dc3cc6436 AS frontend-build

WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2 AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    ANIU_DATA_DIR=/app/data \
    ANIU_DATABASE_URL=sqlite+aiosqlite:////app/data/aniu.sqlite3 \
    ANIU_FRONTEND_DIST=/app/frontend/dist \
    ANIU_SERVE_FRONTEND=1 \
    ANIU_ENABLE_SCHEDULER=1

WORKDIR /app

COPY requirements.lock ./
COPY backend ./backend
COPY --from=frontend-build /build/frontend/dist ./frontend/dist

RUN pip install --no-cache-dir --require-hashes -r requirements.lock \
    && groupadd --system --gid 10001 aniu \
    && useradd --system --uid 10001 --gid aniu \
        --home-dir /nonexistent --no-create-home --shell /usr/sbin/nologin aniu \
    && mkdir -p /app/data \
    && chown -R aniu:aniu /app

USER aniu

EXPOSE 8000
VOLUME ["/app/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/ready', timeout=4)"]

ENTRYPOINT ["python", "-m", "backend.serve"]
CMD ["--host", "0.0.0.0", "--port", "8000"]
