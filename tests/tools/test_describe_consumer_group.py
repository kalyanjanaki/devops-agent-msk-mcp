from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from msk_mcp.tools.describe_consumer_group import describe_consumer_group


class _Future:
    def __init__(self, value, exc=None):
        self._value = value
        self._exc = exc

    def result(self, timeout=None):
        if self._exc:
            raise self._exc
        return self._value


def _state(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name)


def _factory(admin) -> MagicMock:
    f = MagicMock()
    f.get.return_value = admin
    return f


def _admin_with_describe(desc) -> MagicMock:
    admin = MagicMock()
    admin.describe_consumer_groups.return_value = {"g1": _Future(desc)}
    # Default: no offsets — test offset-less path. Tests that need offsets override this.
    admin.list_consumer_group_offsets.return_value = {}
    admin.list_offsets = None
    return admin


async def test_describe_returns_state_and_members():
    desc = SimpleNamespace(
        state=_state("STABLE"),
        protocol_type="consumer",
        coordinator=SimpleNamespace(id=7),
        members=[
            SimpleNamespace(
                member_id="m-1",
                host="/10.0.0.1",
                client_id="my-client",
                assignment=SimpleNamespace(
                    topic_partitions=[
                        SimpleNamespace(topic="t", partition=0),
                        SimpleNamespace(topic="t", partition=1),
                    ]
                ),
            ),
        ],
    )
    admin = _admin_with_describe(desc)

    result = await describe_consumer_group(
        factory=_factory(admin),
        cluster_id="poc-dev",
        group_id="g1",
        include_offsets=False,
    )
    assert result["state"] == "STABLE"
    assert result["coordinator"] == 7
    assert result["protocol_type"] == "consumer"
    assert result["is_rebalancing"] is False
    assert len(result["members"]) == 1
    assert result["members"][0]["consumer_id"] == "m-1"
    assert len(result["members"][0]["assigned_partitions"]) == 2


async def test_is_rebalancing_flag():
    desc = SimpleNamespace(
        state=_state("CompletingRebalance"),
        protocol_type="consumer",
        coordinator=None,
        members=[],
    )
    admin = _admin_with_describe(desc)
    result = await describe_consumer_group(
        factory=_factory(admin),
        cluster_id="poc-dev",
        group_id="g1",
        include_offsets=False,
    )
    assert result["is_rebalancing"] is True


async def test_unknown_group_returns_invalid_params_envelope():
    admin = MagicMock()
    admin.describe_consumer_groups.return_value = {}  # group not present
    result = await describe_consumer_group(
        factory=_factory(admin),
        cluster_id="poc-dev",
        group_id="missing",
    )
    assert result["error"] is True
    assert result["error_type"] == "INVALID_PARAMS"
