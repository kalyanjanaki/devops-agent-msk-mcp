from __future__ import annotations

import asyncio
import socket
from unittest.mock import patch

import pytest

from msk_mcp.config import load_registry
from msk_mcp.errors import ErrorType, MskToolError
from msk_mcp.tools.test_broker_connectivity import (
    _classify_protocol_failure,
    _parse_endpoint,
    probe_broker_connectivity,
)


def _registry(tmp_path):
    p = tmp_path / "clusters.yaml"
    p.write_text(
        """
clusters:
  poc-dev:
    bootstrap_servers: b-1.example:9098
    region: us-east-1
    auth_type: IAM
"""
    )
    return load_registry(p)


# --- _parse_endpoint ---


def test_parse_endpoint_host_port():
    assert _parse_endpoint("b-1.example.com:9098") == ("b-1.example.com", 9098)


def test_parse_endpoint_with_scheme():
    assert _parse_endpoint("https://b-1.example.com:9098") == ("b-1.example.com", 9098)


def test_parse_endpoint_rejects_no_port():
    with pytest.raises(MskToolError) as ei:
        _parse_endpoint("b-1.example.com")
    assert ei.value.error_type == ErrorType.INVALID_PARAMS


def test_parse_endpoint_rejects_non_numeric_port():
    with pytest.raises(MskToolError):
        _parse_endpoint("b-1.example.com:abcd")


# --- _classify_protocol_failure ---


@pytest.mark.parametrize(
    "msg,expected_stage",
    [
        ("SSL handshake failed: certificate verify", "TLS"),
        ("SASL authentication failed", "SASL"),
        ("Authentication failed via OAUTHBEARER", "SASL"),
        ("Connection timed out during metadata fetch", "PROTOCOL"),
        ("Unsupported version", "PROTOCOL"),
        ("Some weird thing", "PROTOCOL"),
    ],
)
def test_classify_protocol_failure(msg, expected_stage):
    stage, _ = _classify_protocol_failure(Exception(msg))
    assert stage == expected_stage


# --- TCP probe path ---


async def test_returns_network_failure_on_dns_error(tmp_path):
    reg = _registry(tmp_path)
    # Use an obviously-bad hostname; getaddrinfo will fail.
    result = await probe_broker_connectivity(
        registry=reg,
        cluster_id="poc-dev",
        broker_endpoint="this-host-definitely-does-not-exist.invalid:9098",
        timeout=2.0,
    )
    assert result["connection_successful"] is False
    assert result["failure_stage"] == "NETWORK"
    assert "this-host-definitely-does-not-exist" in result["failure_detail"] or "DNS" in result["failure_detail"] or "resolution" in result["failure_detail"].lower()


async def test_returns_network_failure_on_connection_refused(tmp_path):
    reg = _registry(tmp_path)
    # Pick a port nothing is listening on. 1 is reliably refused on macOS/Linux.
    result = await probe_broker_connectivity(
        registry=reg,
        cluster_id="poc-dev",
        broker_endpoint="127.0.0.1:1",
        timeout=2.0,
    )
    assert result["connection_successful"] is False
    assert result["failure_stage"] == "NETWORK"


async def test_unknown_cluster_returns_envelope(tmp_path):
    reg = _registry(tmp_path)
    result = await probe_broker_connectivity(
        registry=reg,
        cluster_id="missing",
        broker_endpoint="b-1.example:9098",
        timeout=2.0,
    )
    assert result["error"] is True


async def test_invalid_endpoint_returns_envelope(tmp_path):
    reg = _registry(tmp_path)
    result = await probe_broker_connectivity(
        registry=reg,
        cluster_id="poc-dev",
        broker_endpoint="not-a-valid-endpoint",
        timeout=2.0,
    )
    assert result["error"] is True
    assert result["error_type"] == "INVALID_PARAMS"


# --- protocol probe path (TCP succeeds, then we mock the AdminClient) ---


async def test_protocol_failure_is_classified_as_sasl(tmp_path, monkeypatch):
    """Open a TCP listener so the TCP probe passes, then make AdminClient raise."""

    class _MockAdminClient:
        def __init__(self, conf):
            pass

        def list_topics(self, timeout=None):
            raise Exception("SASL authentication failed: token expired")

    monkeypatch.setattr(
        "msk_mcp.tools.test_broker_connectivity.AdminClient", _MockAdminClient
    )

    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        reg = _registry(tmp_path)
        result = await probe_broker_connectivity(
            registry=reg,
            cluster_id="poc-dev",
            broker_endpoint=f"127.0.0.1:{port}",
            timeout=2.0,
        )
        assert result["connection_successful"] is False
        assert result["failure_stage"] == "SASL"
    finally:
        server.close()
        await server.wait_closed()


async def test_protocol_success_returns_ok(tmp_path, monkeypatch):
    class _MockAdminClient:
        def __init__(self, conf):
            pass

        def list_topics(self, timeout=None):
            from types import SimpleNamespace
            return SimpleNamespace(topics={})

    monkeypatch.setattr(
        "msk_mcp.tools.test_broker_connectivity.AdminClient", _MockAdminClient
    )

    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        reg = _registry(tmp_path)
        result = await probe_broker_connectivity(
            registry=reg,
            cluster_id="poc-dev",
            broker_endpoint=f"127.0.0.1:{port}",
            timeout=2.0,
        )
        assert result["connection_successful"] is True
        assert result["failure_stage"] is None
    finally:
        server.close()
        await server.wait_closed()
