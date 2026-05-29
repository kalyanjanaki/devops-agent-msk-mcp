from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from msk_mcp.server import create_mcp


class _Future:
    def __init__(self, value):
        self._value = value

    def result(self, timeout=None):
        return self._value


def _state(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name)


def _node(node_id: int) -> SimpleNamespace:
    return SimpleNamespace(id=node_id)


def _wire_factory_to_admin(ctx, admin) -> None:
    ctx.factory.get = lambda cluster_id: admin


async def _call_tool(mcp, name: str, args: dict) -> dict:
    """FastMCP.call_tool returns a list of MCP content blocks; we want the JSON dict.

    Different SDK versions return different envelope shapes. This helper accepts both.
    """
    raw = await mcp.call_tool(name, args)
    # Newer SDKs may return a tuple (content_list, structured_data); older return list.
    if isinstance(raw, tuple):
        contents, structured = raw
        if structured:
            return structured
        items = contents
    else:
        items = raw

    # Find the first text content and parse it.
    for item in items or []:
        text = getattr(item, "text", None)
        if text:
            return json.loads(text)
    raise AssertionError(f"No text content in tool result: {raw!r}")


async def test_list_consumer_groups_via_mcp(app_context):
    listing = SimpleNamespace(
        valid=[
            SimpleNamespace(group_id="g1", state=_state("STABLE")),
            SimpleNamespace(group_id="g2", state=_state("EMPTY")),
        ],
        errors=[],
    )
    admin = MagicMock()
    admin.list_consumer_groups.return_value = _Future(listing)
    _wire_factory_to_admin(app_context, admin)

    mcp = create_mcp(app_context)
    result = await _call_tool(mcp, "list_consumer_groups", {"cluster_id": "poc-dev"})
    assert result["consumer_groups"] == ["g1", "g2"]
    assert result["total_count"] == 2


async def test_describe_consumer_group_via_mcp(app_context):
    desc = SimpleNamespace(
        state=_state("STABLE"),
        protocol_type="consumer",
        coordinator=SimpleNamespace(id=3),
        members=[
            SimpleNamespace(
                member_id="m-1",
                host="/10.0.0.1",
                client_id="my-client",
                assignment=SimpleNamespace(
                    topic_partitions=[SimpleNamespace(topic="t", partition=0)]
                ),
            )
        ],
    )
    admin = MagicMock()
    admin.describe_consumer_groups.return_value = {"g1": _Future(desc)}
    admin.list_consumer_group_offsets.return_value = {}
    admin.list_offsets = None
    _wire_factory_to_admin(app_context, admin)

    mcp = create_mcp(app_context)
    result = await _call_tool(
        mcp,
        "describe_consumer_group",
        {"cluster_id": "poc-dev", "group_id": "g1", "include_offsets": False},
    )
    assert result["state"] == "STABLE"
    assert result["coordinator"] == 3
    assert len(result["members"]) == 1


async def test_describe_topic_via_mcp(app_context):
    desc = SimpleNamespace(
        partitions=[
            SimpleNamespace(
                id=0,
                leader=_node(1),
                replicas=[_node(1), _node(2)],
                isr=[_node(1), _node(2)],
            )
        ]
    )
    admin = MagicMock()
    admin.describe_topics.return_value = {"orders": _Future(desc)}
    _wire_factory_to_admin(app_context, admin)

    mcp = create_mcp(app_context)
    result = await _call_tool(
        mcp,
        "describe_topic",
        {"cluster_id": "poc-dev", "topic_name": "orders"},
    )
    assert result["topic"] == "orders"
    assert result["partition_count"] == 1
    assert result["partitions"][0]["is_under_replicated"] is False


async def test_unknown_cluster_returns_envelope(app_context):
    """Tool should return a structured error envelope, not crash."""
    def _raise(_cluster_id):
        from msk_mcp.config import UnknownClusterError
        raise UnknownClusterError("nope", ["poc-dev"])

    app_context.factory.get = _raise
    mcp = create_mcp(app_context)
    result = await _call_tool(
        mcp, "list_consumer_groups", {"cluster_id": "nope"}
    )
    assert result["error"] is True
    assert result["error_type"] == "EXECUTION_FAILURE"
