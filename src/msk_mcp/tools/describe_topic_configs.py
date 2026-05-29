from __future__ import annotations

import asyncio
from typing import Any

from msk_mcp.errors import ErrorType, MskToolError, tool_error_handler
from msk_mcp.kafka_clients import AdminClientFactory

# Configs we explicitly highlight when their source != DEFAULT_CONFIG.
# These are the ones that most often cause silent perf/correctness issues.
_NOTABLE_KEYS = frozenset(
    {
        "compression.type",
        "cleanup.policy",
        "retention.ms",
        "retention.bytes",
        "min.insync.replicas",
        "max.message.bytes",
        "segment.ms",
        "segment.bytes",
        "unclean.leader.election.enable",
        "message.timestamp.type",
    }
)


@tool_error_handler
async def describe_topic_configs(
    *,
    factory: AdminClientFactory,
    cluster_id: str,
    topic_name: str,
) -> dict[str, Any]:
    """Topic-level configuration as the broker sees it right now.

    Returns each config with its source (DYNAMIC_TOPIC_CONFIG vs DEFAULT vs
    STATIC_BROKER_CONFIG) — the field that answers 'is this an override or
    inherited?'. Surfaces notable_overrides for quick triage of the configs
    that most often cause silent perf/correctness issues (compression.type,
    cleanup.policy, min.insync.replicas, etc.).
    """
    admin = factory.get(cluster_id)
    loop = asyncio.get_running_loop()

    resource = _build_topic_resource(topic_name)

    futures = await loop.run_in_executor(None, lambda: admin.describe_configs([resource]))
    if not futures:
        raise MskToolError(
            ErrorType.INVALID_PARAMS,
            f"Topic not found or not describable: {topic_name}",
        )

    # describe_configs returns dict keyed by ConfigResource → future yielding dict[str, ConfigEntry]
    fut = next(iter(futures.values()))
    config_entries = await loop.run_in_executor(None, lambda: fut.result())

    configs_out: list[dict[str, Any]] = []
    notable: list[dict[str, Any]] = []

    for name, entry in (config_entries or {}).items():
        source = _source_name(getattr(entry, "source", None))
        # is_default = the value is inherited from a default. Two signals:
        # the entry's is_default flag (when the SDK exposes it), OR the source
        # being one of the *_DEFAULT_* enum values.
        source_is_default = source is not None and "DEFAULT" in source
        record = {
            "name": name,
            "value": getattr(entry, "value", None),
            "source": source,
            "is_sensitive": bool(getattr(entry, "is_sensitive", False)),
            "is_read_only": bool(getattr(entry, "is_read_only", False)),
            "is_default": bool(getattr(entry, "is_default", False)) or source_is_default,
        }
        configs_out.append(record)
        if name in _NOTABLE_KEYS and not record["is_default"]:
            notable.append({"name": name, "value": record["value"], "source": source})

    configs_out.sort(key=lambda c: c["name"])
    notable.sort(key=lambda c: c["name"])

    return {
        "cluster_id": cluster_id,
        "topic": topic_name,
        "configs": configs_out,
        "notable_overrides": notable,
        "summary": _summarize(topic_name, len(configs_out), notable),
    }


def _build_topic_resource(topic_name: str) -> Any:
    """Build the ConfigResource for a topic. Newer SDK uses ResourceType enum."""
    from confluent_kafka.admin import ConfigResource

    # Newer: ConfigResource(ResourceType.TOPIC, name); older: ConfigResource('topic', name).
    try:
        from confluent_kafka.admin import ResourceType  # type: ignore
        return ConfigResource(ResourceType.TOPIC, topic_name)
    except ImportError:
        return ConfigResource("topic", topic_name)


def _source_name(source: Any) -> str | None:
    """Resolve the config source to its enum name.

    Some confluent-kafka versions return the ConfigSource enum directly (which
    has a .name); others return the underlying int value. We tolerate both.
    """
    if source is None:
        return None
    if hasattr(source, "name") and source.name:
        return source.name
    if isinstance(source, int):
        try:
            from confluent_kafka.admin import ConfigSource  # type: ignore
            return ConfigSource(source).name
        except (ImportError, ValueError):
            return f"UNKNOWN({source})"
    return str(source)


def _summarize(topic: str, total: int, notable: list[dict[str, Any]]) -> str:
    if not notable:
        return f"topic={topic}, configs={total}, all defaults"
    keys = ", ".join(n["name"] for n in notable)
    return f"topic={topic}, configs={total}, notable_overrides=[{keys}]"
