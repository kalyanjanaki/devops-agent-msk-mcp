from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from msk_mcp.tools.list_consumer_groups import list_consumer_groups


class _Future:
    def __init__(self, value):
        self._value = value

    def result(self, timeout=None):
        return self._value


def _state(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name)


def _factory_returning(admin_mock) -> MagicMock:
    f = MagicMock()
    f.get.return_value = admin_mock
    return f


async def test_list_consumer_groups_returns_all_when_no_filter():
    listing = SimpleNamespace(
        valid=[
            SimpleNamespace(group_id="g1", state=_state("STABLE")),
            SimpleNamespace(group_id="g2", state=_state("EMPTY")),
        ],
        errors=[],
    )
    admin = MagicMock()
    admin.list_consumer_groups.return_value = _Future(listing)

    result = await list_consumer_groups(
        factory=_factory_returning(admin),
        cluster_id="poc-dev",
    )
    assert result["cluster_id"] == "poc-dev"
    assert result["consumer_groups"] == ["g1", "g2"]
    assert result["total_count"] == 2
    assert any(g["state"] == "STABLE" for g in result["groups_with_state"])


async def test_list_consumer_groups_state_filter():
    listing = SimpleNamespace(
        valid=[
            SimpleNamespace(group_id="g1", state=_state("STABLE")),
            SimpleNamespace(group_id="g2", state=_state("EMPTY")),
            SimpleNamespace(group_id="g3", state=_state("STABLE")),
        ],
        errors=[],
    )
    admin = MagicMock()
    admin.list_consumer_groups.return_value = _Future(listing)

    result = await list_consumer_groups(
        factory=_factory_returning(admin),
        cluster_id="poc-dev",
        state_filter="stable",
    )
    assert result["consumer_groups"] == ["g1", "g3"]
    assert result["total_count"] == 2


async def test_list_consumer_groups_handles_unknown_cluster():
    f = MagicMock()
    f.get.side_effect = Exception("boom")  # generic exception path
    result = await list_consumer_groups(factory=f, cluster_id="nope")
    assert result["error"] is True


async def test_summary_includes_state_breakdown():
    listing = SimpleNamespace(
        valid=[
            SimpleNamespace(group_id="g1", state=_state("STABLE")),
            SimpleNamespace(group_id="g2", state=_state("EMPTY")),
            SimpleNamespace(group_id="g3", state=_state("STABLE")),
        ],
        errors=[],
    )
    admin = MagicMock()
    admin.list_consumer_groups.return_value = _Future(listing)

    result = await list_consumer_groups(
        factory=_factory_returning(admin), cluster_id="poc-dev"
    )
    assert "summary" in result
    assert "3 group(s)" in result["summary"]
    assert "2 STABLE" in result["summary"]
    assert "1 EMPTY" in result["summary"]


async def test_summary_when_no_groups():
    listing = SimpleNamespace(valid=[], errors=[])
    admin = MagicMock()
    admin.list_consumer_groups.return_value = _Future(listing)

    result = await list_consumer_groups(
        factory=_factory_returning(admin), cluster_id="poc-dev"
    )
    assert result["total_count"] == 0
    assert result["summary"] == "No consumer groups on this cluster"


async def test_summary_when_state_filter_yields_zero():
    listing = SimpleNamespace(
        valid=[SimpleNamespace(group_id="g1", state=_state("EMPTY"))],
        errors=[],
    )
    admin = MagicMock()
    admin.list_consumer_groups.return_value = _Future(listing)

    result = await list_consumer_groups(
        factory=_factory_returning(admin),
        cluster_id="poc-dev",
        state_filter="STABLE",
    )
    assert result["total_count"] == 0
    assert "STABLE" in result["summary"]
    assert "No consumer groups" in result["summary"]


async def test_summary_mentions_listing_errors_when_present():
    listing = SimpleNamespace(
        valid=[SimpleNamespace(group_id="g1", state=_state("STABLE"))],
        errors=["transient broker error"],
    )
    admin = MagicMock()
    admin.list_consumer_groups.return_value = _Future(listing)

    result = await list_consumer_groups(
        factory=_factory_returning(admin), cluster_id="poc-dev"
    )
    assert result["errors"] == ["transient broker error"]
    assert "1 listing error" in result["summary"]
