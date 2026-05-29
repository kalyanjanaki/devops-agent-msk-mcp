from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import MagicMock

from msk_mcp.tools.describe_partition_reassignments import (
    describe_partition_reassignments,
)


class _Future:
    def __init__(self, value):
        self._value = value

    def result(self, timeout=None):
        return self._value


def _factory(admin) -> MagicMock:
    f = MagicMock()
    f.get.return_value = admin
    return f


@dataclass(frozen=True)
class _TP:
    topic: str
    partition: int


@dataclass
class _Info:
    replicas: list = field(default_factory=list)
    adding_replicas: list = field(default_factory=list)
    removing_replicas: list = field(default_factory=list)


def _tp(topic, partition):
    return _TP(topic=topic, partition=partition)


def _info(replicas, adding, removing):
    return _Info(
        replicas=replicas,
        adding_replicas=adding,
        removing_replicas=removing,
    )


async def test_returns_empty_when_no_reassignments():
    admin = MagicMock()
    admin.list_partition_reassignments.return_value = _Future(
        SimpleNamespace(partition_reassignments={})
    )
    result = await describe_partition_reassignments(
        factory=_factory(admin), cluster_id="poc-dev"
    )
    assert result["total_count"] == 0
    assert "No partition reassignments" in result["summary"]


async def test_returns_in_progress_reassignments_sorted():
    raw = {
        _tp("orders", 1): _info([1, 2, 3], [3], [1]),
        _tp("orders", 0): _info([1, 2], [], []),
        _tp("events", 0): _info([4, 5], [5], [4]),
    }
    admin = MagicMock()
    admin.list_partition_reassignments.return_value = _Future(
        SimpleNamespace(partition_reassignments=raw)
    )
    result = await describe_partition_reassignments(
        factory=_factory(admin), cluster_id="poc-dev"
    )
    assert result["total_count"] == 3
    # Sorted by (topic, partition)
    keys = [(r["topic"], r["partition"]) for r in result["in_progress_reassignments"]]
    assert keys == [("events", 0), ("orders", 0), ("orders", 1)]
    orders_1 = next(
        r for r in result["in_progress_reassignments"]
        if r["topic"] == "orders" and r["partition"] == 1
    )
    assert orders_1["adding_replicas"] == [3]
    assert orders_1["removing_replicas"] == [1]


async def test_topic_filter():
    raw = {
        _tp("orders", 0): _info([1, 2], [2], [1]),
        _tp("events", 0): _info([3, 4], [4], [3]),
    }
    admin = MagicMock()
    admin.list_partition_reassignments.return_value = _Future(
        SimpleNamespace(partition_reassignments=raw)
    )
    result = await describe_partition_reassignments(
        factory=_factory(admin), cluster_id="poc-dev", topic_filter="orders"
    )
    assert result["total_count"] == 1
    assert result["in_progress_reassignments"][0]["topic"] == "orders"


async def test_unsupported_sdk_returns_clean_response():
    admin = MagicMock(spec=[])  # no list_partition_reassignments attr
    result = await describe_partition_reassignments(
        factory=_factory(admin), cluster_id="poc-dev"
    )
    assert result["total_count"] == 0
    assert "not available" in result["summary"]


async def test_handles_non_future_return():
    """Older SDKs may return the result object directly."""
    raw = {_tp("orders", 0): _info([1, 2], [2], [1])}
    admin = MagicMock()
    admin.list_partition_reassignments.return_value = SimpleNamespace(
        partition_reassignments=raw
    )  # no .result()
    result = await describe_partition_reassignments(
        factory=_factory(admin), cluster_id="poc-dev"
    )
    assert result["total_count"] == 1
