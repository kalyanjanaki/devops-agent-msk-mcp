from __future__ import annotations

import logging
import sys
import uuid
from contextvars import ContextVar

import structlog
from starlette.types import ASGIApp, Receive, Scope, Send

correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level.upper(),
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _inject_correlation_id,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def _inject_correlation_id(_logger: object, _name: str, event_dict: dict) -> dict:
    cid = correlation_id_var.get()
    if cid:
        event_dict.setdefault("correlation_id", cid)
    return event_dict


class CorrelationIdMiddleware:
    """ASGI middleware that pulls the correlation ID from headers (or generates one)
    and stores it in the contextvar so log lines and downstream code can pick it up."""

    HEADER_NAMES = (b"x-amzn-trace-id", b"x-correlation-id", b"x-request-id")

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        cid = self._extract_cid(scope) or uuid.uuid4().hex
        token = correlation_id_var.set(cid)
        try:
            await self.app(scope, receive, send)
        finally:
            correlation_id_var.reset(token)

    def _extract_cid(self, scope: Scope) -> str | None:
        headers: list[tuple[bytes, bytes]] = scope.get("headers", [])
        for name, value in headers:
            if name.lower() in self.HEADER_NAMES:
                return value.decode("latin-1")
        return None


def current_correlation_id() -> str | None:
    return correlation_id_var.get()
