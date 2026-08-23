"""FastAPI authentication dependencies for one local identity."""

from __future__ import annotations

from typing import Annotated, Protocol, cast

from fastapi import Depends, Request, Security
from fastapi.security import APIKeyCookie
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.api.db import get_db_session
from backend.business.auth.service import (
    CSRF_HEADER,
    SESSION_COOKIE,
    AuthAppService,
    SessionPrincipal,
    UnauthorizedError,
)

_session_cookie = APIKeyCookie(
    name=SESSION_COOKIE,
    scheme_name="AniuSessionCookie",
    auto_error=False,
)


class AuthRuntimePort(Protocol):
    def auth_service(self, session: AsyncSession) -> AuthAppService: ...

    def require_session_factory(self) -> async_sessionmaker[AsyncSession]: ...


def get_auth_app_service(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AuthAppService:
    runtime = cast(AuthRuntimePort, request.app.state.runtime)
    return runtime.auth_service(session)


async def get_optional_principal(
    auth: Annotated[AuthAppService, Depends(get_auth_app_service)],
    raw: Annotated[str | None, Security(_session_cookie)],
) -> SessionPrincipal | None:
    return await auth.resolve_session(raw)


async def require_authenticated(
    request: Request,
    auth: Annotated[AuthAppService, Depends(get_auth_app_service)],
    principal: Annotated[SessionPrincipal | None, Depends(get_optional_principal)],
) -> SessionPrincipal | None:
    """Require the local identity after first-run initialization."""

    if not await auth.identity_initialized():
        return principal
    if principal is None:
        raise UnauthorizedError("authentication required")
    request.state.principal = principal
    if request.method.upper() not in {"GET", "HEAD", "OPTIONS"}:
        auth.verify_csrf(principal, request.headers.get(CSRF_HEADER))
    return principal


async def require_stream_authenticated(
    request: Request,
    raw: Annotated[str | None, Security(_session_cookie)],
) -> SessionPrincipal | None:
    """Authenticate SSE without retaining a request-scoped database session."""

    runtime = cast(AuthRuntimePort, request.app.state.runtime)
    async with runtime.require_session_factory()() as session:
        auth = runtime.auth_service(session)
        principal = await auth.resolve_session(raw)
        if not await auth.identity_initialized():
            return principal
        if principal is None:
            raise UnauthorizedError("authentication required")
        request.state.principal = principal
        return principal
