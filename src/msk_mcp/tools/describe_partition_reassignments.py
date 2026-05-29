from __future__ import annotations

import asyncio
from typing import Any

from msk_mcp.errors import tool_error_handler
from msk_mcp.kafka_clients import AdminClientFactory


@tool_error_handler
async def describe_partition_reassignments(
    *,
    factory: AdminClientFactory,
    cluster_id: str,
    topic_filter: str | None = None,
) -> dict[str, Any]:
    """In-progress partition reassignments as the controller sees them.

    Direct view of the dedicated Kafka API — more authoritative than inferring
    from kafka-log-dirs.sh's isFuture flag. For each in-progress reassignment,
    returns adding_replicas / removing_replicas so the agent can spot ones
    that have been queued forever (stuck), and so it knows which broker pair
    is moving data.
    """
    admin = factory.get(cluster_id)
    loop = asyncio.get_running_loop()

    list_fn = getattr(admin, "list_partition_reassignments", None)
    if not list_fn:
        # Older confluent-kafka versions don't expose this AdminClient method.
        return _unsupported_response(cluster_id)

    fut_or_obj = await loop.run_in_executor(None, lambda: list_fn())
    result = await _resolve(fut_or_obj, loop)

    reassignments_out: list[dict[str, Any]] = []
    raw = getattr(result, "partition_reassignments", None) or {}
    # raw is dict[TopicPartition, PartitionReassignmentInfo] (or similar)
    for tp, info in raw.items():
        topic = getattr(tp, "topic", None)
        partition = getattr(tp, "partition", None)
        if topic_filter and topic != topic_filter:
            continue
        adding = list(getattr(info, "adding_replicas", []) or [])
        removing = list(getattr(info, "removing_replicas", []) or [])
        replicas = list(getattr(info, "replicas", []) or [])
        reassignments_out.append(
            {
                "topic": topic,
                "partition": partition,
                "current_replicas": replicas,
                "adding_replicas": adding,
                "removing_replicas": removing,
            }
        )

    reassignments_out.sort(key=lambda r: (r["topic"] or "", r["partition"] or 0))

    return {
        "cluster_id": cluster_id,
        "in_progress_reassignments": reassignments_out,
        "total_count": len(reassignments_out),
        "summary": _summarize(len(reassignments_out)),
    }


async def _resolve(fut_or_obj: Any, loop: asyncio.AbstractEventLoop) -> Any:
    if hasattr(fut_or_obj, "result"):
        return await loop.run_in_executor(None, lambda: fut_or_obj.result())
    return fut_or_obj


def _unsupported_response(cluster_id: str) -> dict[str, Any]:
    return {
        "cluster_id": cluster_id,
        "in_progress_reassignments": [],
        "total_count": 0,
        "summary": (
            "list_partition_reassignments not available in this confluent-kafka "
            "version; use describe_log_dirs and look for is_future=true partitions."
        ),
    }


def _summarize(total: int) -> str:
    if total == 0:
        return "No partition reassignments in progress"
    return f"{total} partition reassignment(s) in progress"
