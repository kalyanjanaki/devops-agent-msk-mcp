from __future__ import annotations

import asyncio
import socket
from typing import Any
from urllib.parse import urlsplit

from confluent_kafka.admin import AdminClient

from msk_mcp.config import (
    ClusterConfig,
    ClustersRegistry,
    IamCluster,
)
from msk_mcp.errors import ErrorType, MskToolError, tool_error_handler
from msk_mcp.kafka_clients import _iam_config


# Pytest treats `test_*` functions as test cases by default. Rename the impl
# to avoid that; the MCP tool name (what the agent sees) stays unchanged.
@tool_error_handler
async def probe_broker_connectivity(
    *,
    registry: ClustersRegistry,
    cluster_id: str,
    broker_endpoint: str,
    timeout: float,
) -> dict[str, Any]:
    """Probe a single broker endpoint and pinpoint the failure stage.

    Returns failure_stage = NETWORK | TLS | SASL | PROTOCOL | None on success,
    so the agent can immediately tell whether to look at security groups, certs,
    IAM policies, or broker version compatibility.
    """
    cfg = registry.get(cluster_id)
    host, port = _parse_endpoint(broker_endpoint)

    # Stage 1: TCP reachability. Cheap and fail-fast.
    tcp_error = await _probe_tcp(host, port, timeout=min(timeout, 5.0))
    if tcp_error:
        return {
            "cluster_id": cluster_id,
            "broker_endpoint": broker_endpoint,
            "connection_successful": False,
            "failure_stage": "NETWORK",
            "failure_detail": tcp_error,
            "api_versions": [],
            "summary": f"NETWORK failure to {broker_endpoint}: {tcp_error}",
        }

    # Stage 2+: hand off to AdminClient. Failures here are TLS/SASL/PROTOCOL.
    return await _probe_kafka_protocol(
        cluster_id=cluster_id,
        cfg=cfg,
        broker_endpoint=broker_endpoint,
        timeout=timeout,
    )


def _parse_endpoint(endpoint: str) -> tuple[str, int]:
    """Accept 'host:port' or full URLs like 'https://host:port'."""
    if "://" in endpoint:
        u = urlsplit(endpoint)
        if not u.hostname or not u.port:
            raise MskToolError(
                ErrorType.INVALID_PARAMS,
                f"broker_endpoint must include host and port: {endpoint!r}",
            )
        return u.hostname, u.port
    if ":" not in endpoint:
        raise MskToolError(
            ErrorType.INVALID_PARAMS,
            f"broker_endpoint must be host:port: {endpoint!r}",
        )
    host, port_s = endpoint.rsplit(":", 1)
    try:
        port = int(port_s)
    except ValueError as e:
        raise MskToolError(
            ErrorType.INVALID_PARAMS,
            f"broker_endpoint port not numeric: {endpoint!r}",
        ) from e
    return host, port


async def _probe_tcp(host: str, port: int, timeout: float) -> str | None:
    """Returns None on success; an error string on failure."""
    try:
        fut = asyncio.open_connection(host, port)
        reader, writer = await asyncio.wait_for(fut, timeout=timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return None
    except TimeoutError:
        return f"TCP connect timed out after {timeout}s"
    except socket.gaierror as e:
        return f"DNS resolution failed: {e}"
    except OSError as e:
        return f"Connection failed: {e}"


async def _probe_kafka_protocol(
    *,
    cluster_id: str,
    cfg: ClusterConfig,
    broker_endpoint: str,
    timeout: float,
) -> dict[str, Any]:
    """Use a single-bootstrap AdminClient to attempt full handshake.

    A successful list_topics() implies the connection got through TLS, SASL,
    and the protocol exchange. We capture the API version negotiation as a
    side-output for diagnostic value.
    """
    if not isinstance(cfg, IamCluster):
        return _stub_unsupported(cluster_id, broker_endpoint, type(cfg).__name__)

    conf: dict[str, Any] = {
        "bootstrap.servers": broker_endpoint,
        "socket.timeout.ms": int(timeout * 1000),
        # Don't let confluent-kafka spend a long time retrying — we want fast triage.
        "metadata.max.age.ms": int(timeout * 1000),
    }
    conf.update(_iam_config(cfg))

    loop = asyncio.get_running_loop()

    def _attempt() -> tuple[bool, str | None, str | None, list[dict[str, Any]]]:
        try:
            admin = AdminClient(conf)
            md = admin.list_topics(timeout=timeout)
            api_versions = _extract_api_versions(md)
            return True, None, None, api_versions
        except Exception as e:
            stage, detail = _classify_protocol_failure(e)
            return False, stage, detail, []

    ok, stage, detail, api_versions = await loop.run_in_executor(None, _attempt)

    if ok:
        return {
            "cluster_id": cluster_id,
            "broker_endpoint": broker_endpoint,
            "connection_successful": True,
            "failure_stage": None,
            "failure_detail": None,
            "api_versions": api_versions,
            "summary": f"Successful handshake to {broker_endpoint}",
        }

    return {
        "cluster_id": cluster_id,
        "broker_endpoint": broker_endpoint,
        "connection_successful": False,
        "failure_stage": stage,
        "failure_detail": detail,
        "api_versions": [],
        "summary": f"{stage} failure to {broker_endpoint}: {detail}",
    }


def _stub_unsupported(cluster_id: str, broker_endpoint: str, kind: str) -> dict[str, Any]:
    raise MskToolError(
        ErrorType.EXECUTION_FAILURE,
        f"test_broker_connectivity not yet wired for cluster auth_type={kind}; "
        "POC supports IAM only.",
        suggestion="Use IAM-auth clusters for v1, or wire SCRAM/MTLS in kafka_clients.py.",
    )


def _classify_protocol_failure(e: BaseException) -> tuple[str, str]:
    text = str(e).lower()
    if "ssl" in text or "tls" in text or "certificate" in text:
        return "TLS", str(e)
    if "sasl" in text or "authentication" in text or "oauth" in text or "iam" in text:
        return "SASL", str(e)
    if "timed out" in text or "timeout" in text:
        # TCP succeeded earlier so this is a protocol-layer timeout.
        return "PROTOCOL", str(e)
    if "unsupported" in text or "version" in text or "protocol" in text:
        return "PROTOCOL", str(e)
    return "PROTOCOL", str(e)


def _extract_api_versions(md: Any) -> list[dict[str, Any]]:
    """list_topics returns ClusterMetadata; api_versions aren't on it directly,
    but the very fact metadata returned implies version negotiation succeeded.
    Return an empty list rather than fabricating values.
    """
    return []
