from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from msk_mcp.tools.describe_under_replicated_partitions import (
    describe_under_replicated_partitions,
)


class _Future:
    def __init__(self, value, exc=None):
        self._value = value
        self._exc = exc

    def result(self, timeout=None):
        if self._exc:
            raise self._exc
        return self._value


def _node(node_id: int) -> SimpleNamespace:
    return SimpleNamespace(id=node_id)


def _factory(admin) -> MagicMock:
    f = MagicMock()
    f.get.return_value = admin
    return f


def _admin_with(topics: list[str], descriptions: dict) -> MagicMock:
    admin = MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={t: None for t in topics})
    admin.describe_topics.return_value = {t: _Future(descriptions[t]) for t in descriptions}
    return admin


async def test_returns_empty_when_no_topics():
    admin = MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={})
    result = await describe_under_replicated_partitions(
        factory=_factory(admin), cluster_id="poc-dev"
    )
    assert result["total_count"] == 0
    assert result["under_replicated_partitions"] == []


async def test_finds_under_replicated_and_attributes_brokers():
    desc = SimpleNamespace(
        partitions=[
            SimpleNamespace(id=0, leader=_node(1), replicas=[_node(1), _node(2), _node(3)], isr=[_node(1), _node(3)]),  # missing 2
            SimpleNamespace(id=1, leader=_node(2), replicas=[_node(1), _node(2), _node(3)], isr=[_node(1), _node(2), _node(3)]),  # healthy
            SimpleNamespace(id=2, leader=_node(3), replicas=[_node(1), _node(2), _node(3)], isr=[_node(3)]),  # missing 1, 2
        ]
    )
    admin = _admin_with(["orders"], {"orders": desc})

    result = await describe_under_replicated_partitions(
        factory=_factory(admin), cluster_id="poc-dev"
    )

    assert result["total_count"] == 2
    affected = result["under_replicated_partitions"]
    p0 = next(p for p in affected if p["partition"] == 0)
    assert p0["missing_from_isr"] == [2]
    p2 = next(p for p in affected if p["partition"] == 2)
    assert p2["missing_from_isr"] == [1, 2]
    # Broker 2 dropped twice, broker 1 dropped once.
    assert result["broker_drop_counts"] == {"1": 1, "2": 2}


async def test_filters_internal_consumer_offsets_topic():
    desc = SimpleNamespace(partitions=[])
    admin = MagicMock()
    admin.list_topics.return_value = SimpleNamespace(
        topics={"orders": None, "__consumer_offsets": None}
    )
    admin.describe_topics.return_value = {"orders": _Future(desc)}

    await describe_under_replicated_partitions(factory=_factory(admin), cluster_id="poc-dev")

    # Only "orders" should be sent to describe_topics (internal topic skipped).
    args, _ = admin.describe_topics.call_args
    request = args[0]
    if hasattr(request, "topic_names"):
        names = list(request.topic_names)
    else:
        names = list(request)
    assert names == ["orders"]


async def test_topic_filter_narrows_scope():
    descs = {
        "orders": SimpleNamespace(partitions=[]),
        "events": SimpleNamespace(partitions=[]),
    }
    admin = _admin_with(["orders", "events"], descs)
    await describe_under_replicated_partitions(
        factory=_factory(admin), cluster_id="poc-dev", topic_filter="orders"
    )
    args, _ = admin.describe_topics.call_args
    request = args[0]
    names = list(getattr(request, "topic_names", request))
    assert names == ["orders"]


async def test_skips_topics_that_fail_to_describe():
    """ACL denies / transient errors on one topic shouldn't fail the whole call."""
    admin = MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={"a": None, "b": None})
    admin.describe_topics.return_value = {
        "a": _Future(None, exc=Exception("AccessDenied")),
        "b": _Future(SimpleNamespace(partitions=[])),
    }
    result = await describe_under_replicated_partitions(
        factory=_factory(admin), cluster_id="poc-dev"
    )
    # No crash, no under-replicated reported (b had no partitions, a was skipped).
    assert result["total_count"] == 0


async def test_summary_highlights_worst_broker():
    desc = SimpleNamespace(
        partitions=[
            SimpleNamespace(id=i, leader=_node(1), replicas=[_node(1), _node(7)], isr=[_node(1)])
            for i in range(3)
        ]
    )
    admin = _admin_with(["orders"], {"orders": desc})
    result = await describe_under_replicated_partitions(
        factory=_factory(admin), cluster_id="poc-dev"
    )
    assert "broker 7" in result["summary"]
    assert "3" in result["summary"]
