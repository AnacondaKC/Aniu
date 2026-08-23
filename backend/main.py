"""Default ASGI application."""

from backend.bootstrap.app_factory import create_app

app = create_app()

__all__ = ["app", "create_app"]
