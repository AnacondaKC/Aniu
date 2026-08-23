"""Authentication routes for the single local identity."""

from __future__ import annotations

import ipaddress
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field

from backend.api.schemas.error import error_responses
from backend.api.security import get_auth_app_service, get_optional_principal
from backend.business.auth.service import (
    SESSION_COOKIE,
    SESSION_TTL,
    AuthAppService,
    SessionPrincipal,
)
from backend.business.auth.token_policy import TOKEN_MAX_LENGTH, TOKEN_MIN_LENGTH

router = APIRouter(
    prefix="/api/aniu/auth",
    tags=["Auth"],
    responses=error_responses(400, 401, 403, 422),
)


class SetupIdentityRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    token: str = Field(min_length=TOKEN_MIN_LENGTH, max_length=TOKEN_MAX_LENGTH)


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    token: str = Field(min_length=TOKEN_MIN_LENGTH, max_length=TOKEN_MAX_LENGTH)


class SessionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authenticated: bool
    identity_initialized: bool
    username: str | None = None
    csrf_token: str | None = None


def _set_session_cookie(response: Response, raw_token: str, *, secure: bool) -> None:
    response.set_cookie(
        key=SESSION_COOKIE,
        value=raw_token,
        httponly=True,
        samesite="strict",
        secure=secure,
        max_age=int(SESSION_TTL.total_seconds()),
        path="/",
    )


def _is_loopback(request: Request) -> bool:
    client_host = request.client.host if request.client is not None else ""
    try:
        return ipaddress.ip_address(client_host).is_loopback
    except ValueError:
        return client_host in {"localhost", "testclient"}


@router.get("/session", response_model=SessionResponse)
async def get_session(
    auth: Annotated[AuthAppService, Depends(get_auth_app_service)],
    principal: Annotated[SessionPrincipal | None, Depends(get_optional_principal)],
) -> SessionResponse:
    initialized = await auth.identity_initialized()
    if principal is None:
        return SessionResponse(
            authenticated=False,
            identity_initialized=initialized,
        )
    return SessionResponse(
        authenticated=True,
        identity_initialized=initialized,
        username=principal.identity.username,
        csrf_token=principal.csrf_token,
    )


@router.post(
    "/setup",
    status_code=status.HTTP_201_CREATED,
    response_model=SessionResponse,
)
async def setup_identity(
    payload: SetupIdentityRequest,
    request: Request,
    response: Response,
    auth: Annotated[AuthAppService, Depends(get_auth_app_service)],
) -> SessionResponse:
    if not _is_loopback(request):
        from backend.business.auth.service import ForbiddenError

        raise ForbiddenError("identity setup is only allowed on loopback")
    login_result = await auth.setup(payload.token)
    request.state.principal = login_result.principal
    _set_session_cookie(
        response,
        login_result.raw_session_token,
        secure=request.url.scheme == "https",
    )
    return SessionResponse(
        authenticated=True,
        identity_initialized=True,
        username=login_result.principal.identity.username,
        csrf_token=login_result.raw_csrf_token,
    )


@router.post("/login", response_model=SessionResponse)
async def login(
    payload: LoginRequest,
    request: Request,
    response: Response,
    auth: Annotated[AuthAppService, Depends(get_auth_app_service)],
) -> SessionResponse:
    login_result = await auth.login(payload.token)
    request.state.principal = login_result.principal
    _set_session_cookie(
        response,
        login_result.raw_session_token,
        secure=request.url.scheme == "https",
    )
    return SessionResponse(
        authenticated=True,
        identity_initialized=True,
        username=login_result.principal.identity.username,
        csrf_token=login_result.raw_csrf_token,
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    request: Request,
    response: Response,
    auth: Annotated[AuthAppService, Depends(get_auth_app_service)],
) -> None:
    await auth.logout(request.cookies.get(SESSION_COOKIE))
    response.delete_cookie(SESSION_COOKIE, path="/")
