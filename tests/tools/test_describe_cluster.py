from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from msk_mcp.tools.describe_cluster import describe_cluster


class _Future:
    def __init__(self, value):
        self._value = value

    def result(self, timeout=None):
        return self._value


def _factory(admin) -> MagicMock:
    f = MagicMock()
    f.get.return_value = admin
    return f


def _node(node_id: int, host="b", port=9092, rack=None) -> SimpleNamespace:
    return SimpleNamespace(id=node_id, host=host, port=port, rack=rack)


async def test_returns_brokers_controller_and_uuid_future_api():
    """Newer SDK: describe_cluster() returns a future."""
    desc = SimpleNamespace(
        nodes=[_node(1, host="b-1.example", port=9098), _node(2, host="b-2.example", port=9098)],
        controller=SimpleNamespace(id=1),
        cluster_id="msk-uuid-abc",
    )
    admin = MagicMock()
    admin.describe_cluster.return_value = _Future(desc)

    result = await describe_cluster(factory=_factory(admin), cluster_id="poc-dev")
    assert result["controller_id"] == 1
    assert result["broker_count"] == 2
    assert result["cluster_uuid"] == "msk-uuid-abc"
    assert result["brokers"][0]["broker_id"] == 1
    assert result["brokers"][0]["host"] == "b-1.example"


async def test_handles_older_api_no_future():
    """Older SDK: describe_cluster() returns the object directly."""
    desc = SimpleNamespace(
        nodes=[_node(1)],
        controller=SimpleNamespace(id=1),
        cluster_id="uuid",
    )
    admin = MagicMock()
    admin.describe_cluster.return_value = desc  # no .result()

    result = await describe_cluster(factory=_factory(admin), cluster_id="poc-dev")
    assert result["controller_id"] == 1
    assert result["broker_count"] == 1


async def test_handles_missing_controller():
    desc = SimpleNamespace(
        nodes=[_node(1)],
        controller=None,
        cluster_id="uuid",
    )
    admin = MagicMock()
    admin.describe_cluster.return_value = _Future(desc)

    result = await describe_cluster(factory=_factory(admin), cluster_id="poc-dev")
    assert result["controller_id"] is None


async def test_brokers_sorted_by_id():
    desc = SimpleNamespace(
        nodes=[_node(3), _node(1), _node(2)],
        controller=SimpleNamespace(id=1),
        cluster_id="x",
    )
    admin = MagicMock()
    admin.describe_cluster.return_value = _Future(desc)

    result = await describe_cluster(factory=_factory(admin), cluster_id="poc-dev")
    assert [b["broker_id"] for b in result["brokers"]] == [1, 2, 3]


async def test_summary_includes_controller_and_uuid():
    desc = SimpleNamespace(
        nodes=[_node(1), _node(2)],
        controller=SimpleNamespace(id=2),
        cluster_id="msk-abcd",
    )
    admin = MagicMock()
    admin.describe_cluster.return_value = _Future(desc)

    result = await describe_cluster(factory=_factory(admin), cluster_id="poc-dev")
    assert "broker_2" in result["summary"]
    assert "msk-abcd" in result["summary"]
