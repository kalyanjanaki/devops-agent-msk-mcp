from __future__ import annotations

import asyncio
from typing import Any

from msk_mcp.errors import tool_error_handler
from msk_mcp.kafka_clients import AdminClientFactory


@tool_error_handler
async def describe_under_replicated_partitions(
    *,
    factory: AdminClientFactory,
    cluster_id: str,
    topic_filter: str | None = None,
) -> dict[str, Any]:
    """Find all under-replicated partitions on the cluster, with broker attribution.

    For each affected partition, returns which brokers are missing from ISR — answering
    'is broker N consistently dropping out?' without scanning every topic by hand.
    """
    admin = factory.get(cluster_id)
    loop = asyncio.get_running_loop()

    topics = await _list_topics(admin, loop)
    if topic_filter:
        topics = [t for t in topics if t == topic_filter or t.startswith(topic_filter)]

    if not topics:
        return _empty_result(cluster_id, topic_filter)

    descriptions = await _describe_topics(admin, topics, loop)

    under_replicated: list[dict[str, Any]] = []
    broker_drop_counts: dict[int, int] = {}

    for topic_name, desc in descriptions.items():
        for p in getattr(desc, "partitions", []) or []:
            replicas = [_node_id(r) for r in (getattr(p, "replicas", []) or [])]
            isr = [_node_id(i) for i in (getattr(p, "isr", []) or [])]
            replicas = [r for r in replicas if r is not None]
            isr = [i for i in isr if i is not None]
            missing = sorted(set(replicas) - set(isr))
            if missing:
                under_replicated.append(
                    {
                        "topic": topic_name,
                        "partition": getattr(p, "id", getattr(p, "partition", None)),
                        "leader": _node_id(getattr(p, "leader", None)),
                        "replicas": replicas,
                        "isr": isr,
                        "missing_from_isr": missing,
                    }
                )
                for b in missing:
                    broker_drop_counts[b] = broker_drop_counts.get(b, 0) + 1

    return {
        "cluster_id": cluster_id,
        "under_replicated_partitions": under_replicated,
        "total_count": len(under_replicated),
        "broker_drop_counts": {str(k): v for k, v in sorted(broker_drop_counts.items())},
        "summary": _summarize(len(under_replicated), broker_drop_counts),
    }


async def _list_topics(admin: Any, loop: asyncio.AbstractEventLoop) -> list[str]:
    """list_topics() returns ClusterMetadata; topics dict keyed by name."""
    md = await loop.run_in_executor(None, lambda: admin.list_topics(timeout=15))
    topics = list((getattr(md, "topics", None) or {}).keys())
    # Filter out internal topics that aren't usually interesting (still show __amazon_msk_canary).
    return [t for t in topics if not t.startswith("__consumer_offsets")]


async def _describe_topics(
    admin: Any, topics: list[str], loop: asyncio.AbstractEventLoop
) -> dict[str, Any]:
    """Describe topics in one batch; tolerate per-topic failures (e.g. ACL denies)."""
    request = _build_describe_request(topics)
    futures = await loop.run_in_executor(None, lambda: admin.describe_topics(request))
    out: dict[str, Any] = {}
    for topic, fut in futures.items():
        try:
            out[topic] = await loop.run_in_executor(None, lambda f=fut: f.result())
        except Exception:
            # Skip topics we can't describe; under-replicated check just won't include them.
            continue
    return out


def _build_describe_request(topic_names: list[str]) -> Any:
    try:
        from confluent_kafka import TopicCollection  # type: ignore
        return TopicCollection(topic_names=topic_names)
    except ImportError:
        return topic_names


def _node_id(node: Any) -> int | None:
    if node is None:
        return None
    if hasattr(node, "id"):
        return getattr(node, "id", None)
    if isinstance(node, int):
        return node
    return None


def _empty_result(cluster_id: str, topic_filter: str | None) -> dict[str, Any]:
    return {
        "cluster_id": cluster_id,
        "under_replicated_partitions": [],
        "total_count": 0,
        "broker_drop_counts": {},
        "summary": (
            f"No topics matched filter '{topic_filter}'"
            if topic_filter
            else "No topics on cluster"
        ),
    }


def _summarize(total: int, broker_counts: dict[int, int]) -> str:
    if total == 0:
        return "No under-replicated partitions"
    if not broker_counts:
        return f"{total} under-replicated partitions"
    worst = max(broker_counts.items(), key=lambda kv: kv[1])
    return (
        f"{total} under-replicated partitions; broker {worst[0]} missing from ISR "
        f"on {worst[1]} of them"
    )
