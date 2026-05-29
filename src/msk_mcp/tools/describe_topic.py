from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any

from msk_mcp.errors import ErrorType, MskToolError, tool_error_handler
from msk_mcp.kafka_clients import AdminClientFactory


@tool_error_handler
async def describe_topic(
    *,
    factory: AdminClientFactory,
    cluster_id: str,
    topic_name: str,
) -> dict[str, Any]:
    """Describe a topic: partition layout, leader distribution, ISR membership."""
    admin = factory.get(cluster_id)
    loop = asyncio.get_running_loop()

    request = _build_describe_request([topic_name])
    futures = await loop.run_in_executor(None, lambda: admin.describe_topics(request))
    if topic_name not in futures:
        raise MskToolError(
            ErrorType.INVALID_PARAMS,
            f"Topic not found: {topic_name}",
        )
    desc = await loop.run_in_executor(None, lambda: futures[topic_name].result())

    partitions_out: list[dict[str, Any]] = []
    leader_counter: Counter[int] = Counter()
    replication_factors: set[int] = set()

    for p in getattr(desc, "partitions", []) or []:
        leader = _node_id(getattr(p, "leader", None))
        replicas = [_node_id(r) for r in (getattr(p, "replicas", []) or [])]
        isr = [_node_id(i) for i in (getattr(p, "isr", []) or [])]
        replicas = [r for r in replicas if r is not None]
        isr = [i for i in isr if i is not None]
        replication_factors.add(len(replicas))
        if leader is not None:
            leader_counter[leader] += 1
        partitions_out.append(
            {
                "partition": getattr(p, "id", getattr(p, "partition", None)),
                "leader": leader,
                "replicas": replicas,
                "isr": isr,
                "is_under_replicated": len(isr) < len(replicas) if replicas else False,
            }
        )

    rf = next(iter(replication_factors)) if len(replication_factors) == 1 else None

    return {
        "cluster_id": cluster_id,
        "topic": topic_name,
        "partition_count": len(partitions_out),
        "replication_factor": rf,
        "partitions": partitions_out,
        "leader_distribution": {str(k): v for k, v in leader_counter.items()},
        "summary": _summarize(topic_name, partitions_out),
    }


def _build_describe_request(topic_names: list[str]) -> Any:
    """Newer confluent-kafka requires TopicCollection; older accepts list[str]."""
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


def _summarize(topic: str, parts: list[dict[str, Any]]) -> str:
    underrep = sum(1 for p in parts if p["is_under_replicated"])
    return f"topic={topic}, partitions={len(parts)}, under_replicated={underrep}"
