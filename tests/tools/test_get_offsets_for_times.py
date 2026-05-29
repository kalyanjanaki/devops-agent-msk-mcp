from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock

from msk_mcp.tools.get_offsets_for_times import get_offsets_for_times


class _Future:
    def __init__(self, value, exc=None):
        self._value = value
        self._exc = exc

    def result(self, timeout=None):
        if self._exc:
            raise self._exc
        return self._value


def _factory(admin) -> MagicMock:
    f = MagicMock()
    f.get.return_value = admin
    return f


@dataclass(frozen=True)
class _TP:
    topic: str
    partition: int


def _admin_with_metadata(topic: str, partitions: list[int]) -> MagicMock:
    """Build an admin that returns metadata listing 'topic' with the given partitions."""
    admin = MagicMock()
    parts_md = {p: object() for p in partitions}
    admin.list_topics.return_value = SimpleNamespace(
        topics={topic: SimpleNamespace(partitions=parts_md)}
    )
    return admin


async def test_returns_offsets_per_partition():
    admin = _admin_with_metadata("orders", [0, 1, 2])
    futures = {
        _TP("orders", 0): _Future(SimpleNamespace(offset=100, timestamp=1700000000000)),
        _TP("orders", 1): _Future(SimpleNamespace(offset=250, timestamp=1700000000100)),
        _TP("orders", 2): _Future(SimpleNamespace(offset=-1, timestamp=-1)),  # not found
    }
    admin.list_offsets.return_value = futures

    result = await get_offsets_for_times(
        factory=_factory(admin),
        cluster_id="poc-dev",
        topic_name="orders",
        timestamp_ms=1700000000000,
    )
    assert result["topic"] == "orders"
    assert result["timestamp_ms"] == 1700000000000
    assert len(result["offsets"]) == 3
    by_p = {o["partition"]: o for o in result["offsets"]}
    assert by_p[0]["offset"] == 100
    assert by_p[0]["found"] is True
    assert by_p[2]["found"] is False  # -1 means not found


async def test_caller_can_restrict_partitions():
    admin = MagicMock()
    futures = {
        _TP("orders", 1): _Future(SimpleNamespace(offset=42, timestamp=1700000000000)),
    }
    admin.list_offsets.return_value = futures

    result = await get_offsets_for_times(
        factory=_factory(admin),
        cluster_id="poc-dev",
        topic_name="orders",
        timestamp_ms=1700000000000,
        partitions=[1],
    )
    # Did not call list_topics because partitions were explicit.
    admin.list_topics.assert_not_called()
    assert len(result["offsets"]) == 1
    assert result["offsets"][0]["partition"] == 1


async def test_unknown_topic_returns_envelope():
    admin = MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={})
    result = await get_offsets_for_times(
        factory=_factory(admin),
        cluster_id="poc-dev",
        topic_name="missing",
        timestamp_ms=1700000000000,
    )
    assert result["error"] is True
    assert result["error_type"] == "INVALID_PARAMS"


async def test_per_partition_failures_dont_fail_whole_call():
    admin = _admin_with_metadata("orders", [0, 1])
    futures = {
        _TP("orders", 0): _Future(SimpleNamespace(offset=10, timestamp=1700000000000)),
        _TP("orders", 1): _Future(None, exc=Exception("transient broker error")),
    }
    admin.list_offsets.return_value = futures

    result = await get_offsets_for_times(
        factory=_factory(admin),
        cluster_id="poc-dev",
        topic_name="orders",
        timestamp_ms=1700000000000,
    )
    by_p = {o["partition"]: o for o in result["offsets"]}
    assert by_p[0]["found"] is True
    assert by_p[1]["found"] is False
    assert "transient" in by_p[1]["error"]


async def test_summary_when_all_partitions_have_no_messages():
    admin = _admin_with_metadata("orders", [0, 1])
    futures = {
        _TP("orders", 0): _Future(SimpleNamespace(offset=-1, timestamp=-1)),
        _TP("orders", 1): _Future(SimpleNamespace(offset=-1, timestamp=-1)),
    }
    admin.list_offsets.return_value = futures

    result = await get_offsets_for_times(
        factory=_factory(admin),
        cluster_id="poc-dev",
        topic_name="orders",
        timestamp_ms=9999999999999,  # far future
    )
    assert "no messages found" in result["summary"]
