from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from msk_mcp.tools.describe_topic_configs import describe_topic_configs


class _Future:
    def __init__(self, value):
        self._value = value

    def result(self, timeout=None):
        return self._value


def _factory(admin) -> MagicMock:
    f = MagicMock()
    f.get.return_value = admin
    return f


def _entry(name: str, value: str, source: str, is_default: bool = False, is_sensitive: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        value=value,
        source=SimpleNamespace(name=source),
        is_sensitive=is_sensitive,
        is_read_only=False,
        is_default=is_default,
    )


def _admin_with(entries: dict) -> MagicMock:
    admin = MagicMock()
    # describe_configs returns dict[ConfigResource, future]; we use a sentinel key.
    admin.describe_configs.return_value = {object(): _Future(entries)}
    return admin


async def test_returns_configs_sorted_by_name():
    entries = {
        "retention.ms": _entry("retention.ms", "604800000", "DEFAULT_CONFIG", is_default=True),
        "compression.type": _entry("compression.type", "snappy", "DYNAMIC_TOPIC_CONFIG"),
    }
    admin = _admin_with(entries)

    result = await describe_topic_configs(
        factory=_factory(admin), cluster_id="poc-dev", topic_name="orders"
    )
    names = [c["name"] for c in result["configs"]]
    assert names == ["compression.type", "retention.ms"]


async def test_notable_override_surfaced():
    entries = {
        "compression.type": _entry("compression.type", "lz4", "DYNAMIC_TOPIC_CONFIG"),
        "min.insync.replicas": _entry("min.insync.replicas", "2", "DYNAMIC_TOPIC_CONFIG"),
        "retention.ms": _entry("retention.ms", "604800000", "DEFAULT_CONFIG", is_default=True),
        "noise.config": _entry("noise.config", "x", "DYNAMIC_TOPIC_CONFIG"),  # not in NOTABLE_KEYS
    }
    admin = _admin_with(entries)

    result = await describe_topic_configs(
        factory=_factory(admin), cluster_id="poc-dev", topic_name="orders"
    )
    notable_names = [n["name"] for n in result["notable_overrides"]]
    assert "compression.type" in notable_names
    assert "min.insync.replicas" in notable_names
    # Default values (even of notable keys) shouldn't surface as overrides.
    assert "retention.ms" not in notable_names
    # Non-notable keys never surface, override or not.
    assert "noise.config" not in notable_names


async def test_notable_override_excludes_defaults_even_for_notable_keys():
    entries = {
        "compression.type": _entry(
            "compression.type", "producer", "DEFAULT_CONFIG", is_default=True
        )
    }
    admin = _admin_with(entries)

    result = await describe_topic_configs(
        factory=_factory(admin), cluster_id="poc-dev", topic_name="orders"
    )
    assert result["notable_overrides"] == []
    assert "all defaults" in result["summary"]


async def test_summary_lists_notable_keys():
    entries = {
        "compression.type": _entry("compression.type", "lz4", "DYNAMIC_TOPIC_CONFIG"),
        "max.message.bytes": _entry("max.message.bytes", "10000000", "DYNAMIC_TOPIC_CONFIG"),
    }
    admin = _admin_with(entries)

    result = await describe_topic_configs(
        factory=_factory(admin), cluster_id="poc-dev", topic_name="orders"
    )
    assert "compression.type" in result["summary"]
    assert "max.message.bytes" in result["summary"]


async def test_unknown_topic_returns_envelope():
    admin = MagicMock()
    admin.describe_configs.return_value = {}  # empty futures map
    result = await describe_topic_configs(
        factory=_factory(admin), cluster_id="poc-dev", topic_name="missing"
    )
    assert result["error"] is True
    assert result["error_type"] == "INVALID_PARAMS"
