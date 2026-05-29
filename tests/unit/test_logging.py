from __future__ import annotations

import pytest

from msk_mcp.logging_setup import (
    CorrelationIdMiddleware,
    correlation_id_var,
    current_correlation_id,
)


async def test_middleware_uses_existing_header():
    captured: dict = {}

    async def app(scope, receive, send):
        captured["cid"] = current_correlation_id()

    mw = CorrelationIdMiddleware(app)
    scope = {"type": "http", "headers": [(b"x-amzn-trace-id", b"trace-123")]}

    async def receive():
        return {"type": "http.request"}

    async def send(message):
        pass

    await mw(scope, receive, send)
    assert captured["cid"] == "trace-123"


async def test_middleware_generates_when_missing():
    captured: dict = {}

    async def app(scope, receive, send):
        captured["cid"] = current_correlation_id()

    mw = CorrelationIdMiddleware(app)
    scope = {"type": "http", "headers": []}

    async def receive():
        return {"type": "http.request"}

    async def send(message):
        pass

    await mw(scope, receive, send)
    assert captured["cid"] is not None
    assert len(captured["cid"]) == 32  # uuid4 hex


async def test_middleware_clears_after_request():
    async def app(scope, receive, send):
        pass

    mw = CorrelationIdMiddleware(app)
    scope = {"type": "http", "headers": [(b"x-correlation-id", b"abc")]}

    async def receive():
        return {"type": "http.request"}

    async def send(message):
        pass

    await mw(scope, receive, send)
    assert correlation_id_var.get() is None


async def test_middleware_passes_through_non_http():
    called = {"yes": False}

    async def app(scope, receive, send):
        called["yes"] = True

    mw = CorrelationIdMiddleware(app)
    await mw({"type": "lifespan"}, lambda: None, lambda m: None)
    assert called["yes"] is True
