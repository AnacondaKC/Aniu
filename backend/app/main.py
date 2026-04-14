from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date
import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from app.api.router import router as aniu_router
from app.core.config import get_settings
from app.core.rate_limit import rate_limit_middleware
from app.db.database import init_db
from app.services.scheduler_service import scheduler_service
from app.services.trading_calendar_service import trading_calendar_service

logger = logging.getLogger(__name__)

# Directory where the Vite build output is placed (only exists in Docker image)
_STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


@asynccontextmanager
async def app_lifespan(_app: FastAPI):
    init_db()
    current_year = date.today().year
    trading_calendar_service.ensure_years([current_year])
    try:
        trading_calendar_service.ensure_years([current_year + 1])
    except Exception as exc:
        logger.warning("next-year trading calendar warm up skipped: %s", exc)
    scheduler_service.start()
    try:
        yield
    finally:
        scheduler_service.stop()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=app_lifespan)

    if "*" in settings.cors_allow_origins:
        logger.warning(
            "CORS is configured with wildcard '*'. "
            "Set CORS_ALLOW_ORIGINS to specific origins in production."
        )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )

    app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limit_middleware)

    app.include_router(aniu_router)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    # ── Serve frontend static files when built output exists (Docker) ──
    _serve_frontend(app)

    return app


def _serve_frontend(app: FastAPI) -> None:
    """Mount Vite build output and add SPA catch-all fallback.

    Only activates when ``static/index.html`` exists (i.e. inside the Docker
    image).  In local dev mode the directory doesn't exist, so this is a no-op
    and the Vite dev server handles the frontend as before.
    """
    index_html = _STATIC_DIR / "index.html"
    if not _STATIC_DIR.is_dir() or not index_html.is_file():
        return

    logger.info("Serving frontend from %s", _STATIC_DIR)

    # Serve hashed assets (JS/CSS/images) under /assets
    assets_dir = _STATIC_DIR / "assets"
    if assets_dir.is_dir():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    # SPA catch-all: return index.html for frontend routes only.
    # Exclude /api and /health so that unknown API paths still return proper 404.
    # If the requested path matches a real file under static/, serve it directly
    # (e.g. /aniu.ico, /favicon.ico, /robots.txt).
    _index_content = index_html.read_text(encoding="utf-8")

    @app.get("/{full_path:path}", response_model=None, include_in_schema=False)
    async def spa_fallback(request: Request, full_path: str):
        if full_path.startswith(("api/", "health")):
            return HTMLResponse(content="Not Found", status_code=404)

        if full_path == "favicon.ico":
            fallback_favicon = _STATIC_DIR / "aniu.ico"
            if fallback_favicon.is_file():
                return FileResponse(str(fallback_favicon))

        # Serve real static files (favicon, icons, etc.)
        candidate = _STATIC_DIR / full_path
        if (
            full_path
            and candidate.is_file()
            and _STATIC_DIR in candidate.resolve().parents
        ):
            return FileResponse(str(candidate))

        return HTMLResponse(content=_index_content)


app = create_app()
