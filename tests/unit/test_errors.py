from __future__ import annotations

import asyncio

import pytest

from msk_mcp.errors import ErrorType, MskToolError, tool_error_handler


def test_envelope_shape():
    e = MskToolError(ErrorType.AUTH_FAILURE, "boom", suggestion="check creds", raw_stderr="x")
    env = e.to_envelope()
    assert env == {
        "error": True,
        "error_type": "AUTH_FAILURE",
        "error_message": "boom",
        "raw_stderr": "x",
        "suggestion": "check creds",
    }


async def test_handler_returns_envelope_on_msk_error():
    @tool_error_handler
    async def fn() -> dict:
        raise MskToolError(ErrorType.INVALID_PARAMS, "no such topic")

    result = await fn()
    assert result["error"] is True
    assert result["error_type"] == "INVALID_PARAMS"


async def test_handler_maps_asyncio_timeout():
    @tool_error_handler
    async def fn() -> dict:
        raise asyncio.TimeoutError()

    result = await fn()
    assert result["error_type"] == "TIMEOUT"


async def test_handler_passes_success_through():
    @tool_error_handler
    async def fn() -> dict:
        return {"ok": True}

    assert await fn() == {"ok": True}


async def test_handler_wraps_unknown_exception():
    @tool_error_handler
    async def fn() -> dict:
        raise RuntimeError("kaboom")

    result = await fn()
    assert result["error"] is True
    assert result["error_type"] == "EXECUTION_FAILURE"
    assert "kaboom" in result["error_message"]


def _make_kafka_exc(message: str) -> Exception:
    """Synthesize an exception that looks like it came from confluent_kafka."""

    class _FakeKafkaException(Exception):
        pass

    _FakeKafkaException.__module__ = "confluent_kafka.cimpl"
    return _FakeKafkaException(message)


@pytest.mark.parametrize(
    "message,expected_type",
    [
        ("SASL authentication failed", "AUTH_FAILURE"),
        ("TopicAuthorizationException", "AUTHORIZATION"),
        ("Request timed out", "NETWORK_TIMEOUT"),
        ("UnknownTopicOrPartition", "INVALID_PARAMS"),
        ("GroupIdNotFound", "INVALID_PARAMS"),
        ("Some other broker error", "EXECUTION_FAILURE"),
    ],
)
async def test_handler_classifies_kafka_exceptions(message: str, expected_type: str):
    @tool_error_handler
    async def fn() -> dict:
        raise _make_kafka_exc(message)

    result = await fn()
    assert result["error_type"] == expected_type
