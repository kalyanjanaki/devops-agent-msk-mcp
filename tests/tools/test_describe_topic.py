from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from msk_mcp.tools.describe_topic import describe_topic


class _Future:
    def __init__(self, value):
        self._value = value

    def result(self, timeout=None):
        return self._value


def _node(node_id: int) -> SimpleNamespace:
    return SimpleNamespace(id=node_id)


def _factory(admin) -> MagicMock:
    f = MagicMock()
    f.get.return_value = admin
    return f


async def test_describe_topic_normalizes_partitions_and_isr():
    desc = SimpleNamespace(
        partitions=[
            SimpleNamespace(id=0, leader=_node(1), replicas=[_node(1), _node(2), _node(3)], isr=[_node(1), _node(3)]),
            SimpleNamespace(id=1, leader=_node(2), replicas=[_node(1), _node(2), _node(3)], isr=[_node(1), _node(2), _node(3)]),
        ]
    )
    admin = MagicMock()
    admin.describe_topics.return_value = {"orders": _Future(desc)}

    result = await describe_topic(factory=_factory(admin), cluster_id="poc-dev", topic_name="orders")
    assert result["topic"] == "orders"
    assert result["partition_count"] == 2
    assert result["replication_factor"] == 3
    assert result["partitions"][0]["leader"] == 1
    assert result["partitions"][0]["isr"] == [1, 3]
    assert result["partitions"][0]["is_under_replicated"] is True
    assert result["partitions"][1]["is_under_replicated"] is False
    # Leaders distributed: broker 1 has 1 partition, broker 2 has 1 partition
    assert result["leader_distribution"] == {"1": 1, "2": 1}


async def test_describe_topic_unknown_returns_envelope():
    admin = MagicMock()
    admin.describe_topics.return_value = {}
    result = await describe_topic(factory=_factory(admin), cluster_id="poc-dev", topic_name="missing")
    assert result["error"] is True
    assert result["error_type"] == "INVALID_PARAMS"
