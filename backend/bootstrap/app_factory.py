"""Aniu FastAPI application factory and default ASGI app."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.responses import Response
from starlette.types import Scope

from backend.api.errors import register_exception_handlers
from backend.api.routes import (
    account,
    auth,
    market,
    memories,
    memory_dreams,
    runs,
    schedules,
    settings,
)
from backend.api.sse import StreamHub, stream_hub
from backend.bootstrap.runtime import AppRuntime
from backend.bootstrap.runtime_config import RuntimeConfig
from backend.business.settings import AppSettings, default_stage_settings
from backend.infra.db import (
    build_database_url,
    create_engine,
    create_session_factory,
    init_db,
)
from backend.infra.repositories import (
    AuditLogRepository,
    AuditRecord,
    RunRepository,
    ScheduleRepository,
    SettingsRepository,
)
from backend.infra.scheduler import JobRunner
from backend.llm import LLMClient, ModelConnectivityTester

logger = logging.getLogger(__name__)

_AUDITED_ACTIONS: tuple[tuple[str, str, str], ...] = (
    ("POST", "/api/aniu/auth/setup", "auth.setup"),
    ("POST", "/api/aniu/auth/login", "auth.login"),
    ("POST", "/api/aniu/auth/logout", "auth.logout"),
    ("PUT", "/api/aniu/settings", "settings.update"),
    ("POST", "/api/aniu/settings/channels/with-models", "model_channel.create"),
    ("PUT", "/api/aniu/settings/channels/", "model_channel.update"),
    (
        "DELETE",
        "/api/aniu/settings/channels/",
        "model_channel.delete_or_secret_clear",
    ),
    ("POST", "/api/aniu/schedules", "schedule.create"),
    ("PUT", "/api/aniu/schedules/", "schedule.update"),
    ("POST", "/api/aniu/runs/", "run.action"),
)


def _audit_event(method: str, path: str) -> str | None:
    for expected_method, prefix, event_type in sorted(
        _AUDITED_ACTIONS, key=lambda item: len(item[1]), reverse=True
    ):
        if method == expected_method and (path == prefix or path.startswith(prefix)):
            return event_type
    return None


async def _recover_stale_run_jobs(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Fail orphan or expired RUNNING runs before starting the local worker."""

    from backend.business.runs.job import ACTIVE_JOB_STATUSES, RunJobStatus
    from backend.infra.repositories.run_job_repo import RunJobRepository

    failed_any = False
    recovery_worker_id = f"startup-recovery-{uuid4().hex}"
    async with session_factory() as session:
        run_repo = RunRepository(session)
        job_repo = RunJobRepository(session)
        while True:
            zombie = await run_repo.get_running_run()
            if zombie is None:
                break
            job = await job_repo.get_by_run_id(zombie.run_id)
            if job is not None and job.status in ACTIVE_JOB_STATUSES:
                if job.status is RunJobStatus.PENDING:
                    break
                lease_expires_at = job.lease_expires_at
                if lease_expires_at is None or lease_expires_at > datetime.now(tz=UTC):
                    break
                recovered_job = await job_repo.reclaim_expired(
                    zombie.run_id,
                    worker_id=recovery_worker_id,
                    lease_seconds=60.0,
                )
                if recovered_job is None or recovered_job.claim_token is None:
                    break
                reason = "应用启动时发现运行租约已过期；为避免重复交易，任务未自动重放"
                logger.warning(
                    "interrupting RUNNING run with expired job lease",
                    extra={
                        "run_id": zombie.run_id,
                        "stage_id": zombie.current_state.value,
                        "error_code": "expired_run_lease",
                    },
                )
                zombie.fail(reason)
                await run_repo.save(zombie)
                terminal = await job_repo.mark_terminal(
                    zombie.run_id,
                    status=RunJobStatus.INTERRUPTED,
                    worker_id=recovery_worker_id,
                    claim_token=recovered_job.claim_token,
                    error_code="expired_run_lease",
                    error_message=reason,
                )
                if terminal is None:
                    await session.rollback()
                    break
                failed_any = True
                continue
            reason = "应用启动时发现运行没有可恢复的执行任务"
            logger.warning(
                "recovering orphan RUNNING run without active job",
                extra={
                    "run_id": zombie.run_id,
                    "stage_id": zombie.current_state.value,
                    "error_code": "orphan_running_run",
                },
            )
            zombie.fail(reason)
            await run_repo.save(zombie)
            failed_any = True
        if failed_any:
            await session.commit()


class SPAStaticFiles(StaticFiles):
    """Serve the built SPA and fall back to index.html for client routes."""

    async def get_response(self, path: str, scope: Scope) -> Response:
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            request_path = scope.get("path", "")
            is_backend_path = request_path.startswith(("/api/", "/health"))
            if exc.status_code == 404 and not is_backend_path:
                return await super().get_response("index.html", scope)
            raise


async def _initialize_runtime(application: FastAPI, config: RuntimeConfig) -> None:
    runtime: AppRuntime = application.state.runtime
    database_url = config.database_url or build_database_url()
    engine = create_engine(database_url)
    runtime.engine = engine
    await init_db(engine)
    session_factory = create_session_factory(engine)
    runtime.session_factory = session_factory

    runtime.stream_hub = StreamHub()
    runtime.llm_client = LLMClient(read_timeout=config.llm_read_timeout_seconds)
    runtime.model_connectivity_tester = ModelConnectivityTester()
    from backend.infra.integrations.models_dev_catalog import ModelsDevCatalog

    runtime.models_dev_catalog = ModelsDevCatalog()
    runtime.mx_http_client = httpx.AsyncClient(timeout=15.0)
    runtime.public_stock_http_client = httpx.AsyncClient(timeout=15.0)

    runtime.require_mx_clients()
    runtime.require_public_stock_data()

    from backend.bootstrap.schedule_handlers import (
        build_account_refresh_handler,
        build_market_analysis_handler,
        build_memory_dream_handler,
    )
    from backend.infra.workers.memory_dream_worker import MemoryDreamWorker
    from backend.infra.workers.run_worker import build_run_worker

    run_worker = build_run_worker(
        session_factory=session_factory,
        executor_factory=runtime.run_executor,
        abort_registry=runtime.abort_registry,
    )
    runtime.run_worker = run_worker

    dream_worker = MemoryDreamWorker(
        session_factory=session_factory,
        service_factory=runtime.dream_service,
    )
    dream_worker.start()
    await dream_worker.recover_pending()
    runtime.dream_worker = dream_worker

    job_runner = JobRunner(
        session_factory=session_factory,
        market_analysis_handler=build_market_analysis_handler(
            session_factory=session_factory,
            run_service_factory=runtime.run_service,
            enqueue_run=run_worker.submit,
        ),
        memory_dream_handler=build_memory_dream_handler(
            session_factory=session_factory,
            dream_service_factory=runtime.dream_service,
            enqueue_dream=dream_worker.submit,
        ),
        account_refresh_handler=build_account_refresh_handler(
            session_factory=session_factory,
            account_service_factory=runtime.account_service,
        ),
        enabled=config.scheduler_enabled,
    )
    runtime.job_runner = job_runner

    async with session_factory() as session:
        schedules_to_sync = await ScheduleRepository(session).list_schedules()
        settings_repo = SettingsRepository(session)
        settings_record = await settings_repo.get()
        if settings_record is None:
            await settings_repo.save(
                AppSettings(stage_settings=default_stage_settings())
            )
        await session.commit()

    await _recover_stale_run_jobs(session_factory)
    run_worker.start()

    if config.scheduler_enabled:
        await job_runner.sync_all(schedules_to_sync)
        dream_schedule_time = (
            settings_record.dream_schedule_time
            if settings_record is not None
            else AppSettings().dream_schedule_time
        )
        await job_runner.sync_memory_dream_job(dream_schedule_time)
        await job_runner.sync_account_refresh_job()
    else:
        logger.info("scheduler is disabled")


async def _shutdown_runtime(application: FastAPI) -> None:
    runtime: AppRuntime = application.state.runtime

    async def cleanup(name: str, operation: Any) -> None:
        try:
            await operation()
        except Exception:  # noqa: BLE001 - cleanup must continue for other resources
            logger.exception("failed to close runtime resource %s", name)

    if runtime.job_runner is not None:
        await cleanup("job_runner", runtime.job_runner.shutdown)
        runtime.job_runner = None
    if runtime.dream_worker is not None:
        await cleanup("dream_worker", runtime.dream_worker.stop)
        runtime.dream_worker = None
    if runtime.run_worker is not None:
        await cleanup("run_worker", runtime.run_worker.stop)
        runtime.run_worker = None
    if runtime.mx_clients is not None:
        await cleanup("mx_clients", runtime.mx_clients.aclose)
        runtime.mx_clients = None
    if runtime.mx_http_client is not None:
        await cleanup("mx_http_client", runtime.mx_http_client.aclose)
        runtime.mx_http_client = None
    if runtime.public_stock_data is not None:
        await cleanup("public_stock_data", runtime.public_stock_data.aclose)
        runtime.public_stock_data = None
    if runtime.public_stock_http_client is not None:
        await cleanup(
            "public_stock_http_client", runtime.public_stock_http_client.aclose
        )
        runtime.public_stock_http_client = None
    if isinstance(runtime.model_connectivity_tester, ModelConnectivityTester):
        await cleanup(
            "model_connectivity_tester", runtime.model_connectivity_tester.aclose
        )
        runtime.model_connectivity_tester = None
    if runtime.models_dev_catalog is not None:
        await cleanup("models_dev_catalog", runtime.models_dev_catalog.aclose)
        runtime.models_dev_catalog = None
    if isinstance(runtime.llm_client, LLMClient):
        await cleanup("llm_client", runtime.llm_client.aclose)
        runtime.llm_client = None
    if isinstance(runtime.engine, AsyncEngine):
        await cleanup("database_engine", runtime.engine.dispose)
        runtime.engine = None
    runtime.session_factory = None
    runtime.stream_hub = None


def _lifespan(config: RuntimeConfig) -> Any:
    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        try:
            await _initialize_runtime(application, config)
            yield
        finally:
            await _shutdown_runtime(application)

    return lifespan


def create_app(config: RuntimeConfig | None = None) -> FastAPI:
    """Create an isolated FastAPI app and composition root."""

    resolved = config or RuntimeConfig.from_env()
    application = FastAPI(
        title="Aniu",
        description="基于股票交易的可进化智能体系统",
        version="1.0.1",
        lifespan=_lifespan(resolved),
    )
    application.state.runtime = AppRuntime(config=resolved)

    application.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=list(resolved.allowed_hosts),
    )
    application.add_middleware(
        CORSMiddleware,
        allow_origins=list(resolved.cors_origins),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Accept", "Content-Type", "X-CSRF-Token"],
    )

    @application.middleware("http")
    async def security_headers(request: Request, call_next: Any) -> Response:
        request_id = request.headers.get("X-Request-ID") or str(uuid4())
        request.state.request_id = request_id[:128]
        response = await call_next(request)
        assert isinstance(response, Response)
        event_type = _audit_event(request.method.upper(), request.url.path)
        session_factory = request.app.state.runtime.session_factory
        if event_type is not None and session_factory is not None:
            try:
                principal = getattr(request.state, "principal", None)
                identity = None if principal is None else principal.identity
                async with session_factory() as audit_session:
                    await AuditLogRepository(audit_session).append(
                        AuditRecord(
                            event_type=event_type,
                            method=request.method.upper(),
                            path=request.url.path,
                            resource_id=(
                                request.path_params.get("run_id")
                                or request.path_params.get("schedule_id")
                                or request.path_params.get("channel_id")
                                or request.path_params.get("profile_id")
                            ),
                            actor_name=(
                                None if identity is None else identity.username
                            ),
                            request_id=str(
                                getattr(request.state, "request_id", "unassigned")
                            ),
                            source_ip=(
                                None if request.client is None else request.client.host
                            ),
                            status_code=response.status_code,
                        )
                    )
                    await audit_session.commit()
            except Exception:
                logger.exception(
                    "failed to append audit log",
                    extra={
                        "request_id": request.state.request_id,
                        "status": "failed",
                    },
                )
        response.headers.setdefault("X-Request-ID", request.state.request_id)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; img-src 'self' data:; "
            "style-src 'self' 'unsafe-inline'; font-src 'self' data:; "
            "script-src 'self'; connect-src 'self'; "
            "frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
        )
        return response

    register_exception_handlers(application)
    application.include_router(auth.router)
    application.include_router(runs.router)
    application.include_router(account.router)
    application.include_router(schedules.router)
    application.include_router(settings.router)
    application.include_router(memories.router)
    application.include_router(market.router)
    application.include_router(memory_dreams.router)
    application.include_router(stream_hub.router)

    @application.get("/health")
    @application.get("/health/live")
    async def health_live() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/health/ready")
    async def health_ready(request: Request) -> object:
        runtime: AppRuntime = request.app.state.runtime
        session_factory = runtime.session_factory
        engine = runtime.engine
        if session_factory is None or engine is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "not_ready", "reason": "database not initialized"},
            )
        try:
            async with session_factory() as session:
                await session.execute(text("SELECT 1"))
                await session.execute(text("SELECT id FROM app_settings LIMIT 1"))
                await session.execute(
                    text(
                        "CREATE TEMP TABLE IF NOT EXISTS aniu_ready_probe (ok INTEGER)"
                    )
                )
                await session.execute(
                    text("INSERT INTO aniu_ready_probe (ok) VALUES (1)")
                )
                await session.rollback()
            from backend.infra.calendar import SUPPORTED_CALENDAR_YEARS
        except Exception:
            logger.exception("readiness database probe failed")
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "not_ready", "reason": "database unavailable"},
            )
        worker = runtime.run_worker
        config = runtime.config
        checks = {
            "db_initialized": True,
            "db_writable": True,
            "worker_running": bool(
                worker is not None and getattr(worker, "is_running", False)
            ),
            "calendar_years": sorted(SUPPORTED_CALENDAR_YEARS),
            "calendar_covers_next_year": (
                max(SUPPORTED_CALENDAR_YEARS, default=0)
                >= datetime.now(tz=UTC).year + 1
            ),
            "lan_mode": bool(config.lan_mode),
        }
        ready = all(
            bool(checks[key])
            for key in (
                "db_initialized",
                "db_writable",
                "worker_running",
                "calendar_covers_next_year",
            )
        )
        return JSONResponse(
            status_code=(
                status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE
            ),
            content={"status": "ready" if ready else "not_ready", "checks": checks},
        )

    if resolved.frontend_dist.exists() and resolved.serve_frontend:
        application.mount(
            "/",
            SPAStaticFiles(directory=resolved.frontend_dist, html=True),
            name="frontend",
        )

    return application


__all__ = ["create_app"]
