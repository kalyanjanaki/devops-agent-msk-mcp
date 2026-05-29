from __future__ import annotations

import asyncio
from typing import Any

from msk_mcp.errors import ErrorType, MskToolError, tool_error_handler
from msk_mcp.kafka_clients import AdminClientFactory


@tool_error_handler
async def get_offsets_for_times(
    *,
    factory: AdminClientFactory,
    cluster_id: str,
    topic_name: str,
    timestamp_ms: int,
    partitions: list[int] | None = None,
) -> dict[str, Any]:
    """Find each partition's offset at a specific point in time.

    Given epoch_ms (e.g. when an alarm fired), returns the offset of the first
    message with timestamp >= timestamp_ms on each partition. Used during
    incident timeline reconstruction: 'how far behind was the consumer when
    the alarm fired?', 'what's the first message we need to reprocess?'.

    Returns offset = -1 for partitions where no message has a timestamp >=
    the requested time (i.e. the timestamp is in the future for that partition).
    """
    admin = factory.get(cluster_id)
    loop = asyncio.get_running_loop()

    OffsetSpec, TopicPartition = _import_kafka_types()
    target_partitions = await _resolve_partitions(
        admin, topic_name, partitions, loop
    )
    if not target_partitions:
        raise MskToolError(
            ErrorType.INVALID_PARAMS,
            f"No partitions to query for topic {topic_name!r}",
        )

    spec = OffsetSpec.for_timestamp(timestamp_ms)
    request = {TopicPartition(topic_name, p): spec for p in target_partitions}

    futures = await loop.run_in_executor(None, lambda: admin.list_offsets(request))

    results: list[dict[str, Any]] = []
    not_found_count = 0
    for tp, fut in futures.items():
        try:
            res = await loop.run_in_executor(None, lambda f=fut: f.result())
            offset = getattr(res, "offset", None)
            if offset is None or offset < 0:
                not_found_count += 1
            results.append(
                {
                    "partition": getattr(tp, "partition", None),
                    "offset": offset,
                    "timestamp_ms": getattr(res, "timestamp", None),
                    "found": offset is not None and offset >= 0,
                }
            )
        except Exception as e:
            results.append(
                {
                    "partition": getattr(tp, "partition", None),
                    "offset": None,
                    "timestamp_ms": None,
                    "found": False,
                    "error": str(e),
                }
            )

    results.sort(key=lambda r: r["partition"] if r["partition"] is not None else -1)

    return {
        "cluster_id": cluster_id,
        "topic": topic_name,
        "timestamp_ms": timestamp_ms,
        "offsets": results,
        "summary": _summarize(topic_name, timestamp_ms, len(results), not_found_count),
    }


def _import_kafka_types():
    """OffsetSpec lives in confluent_kafka.admin in newer versions, but historically
    was at the top-level package. Tolerate both.
    """
    try:
        from confluent_kafka.admin import OffsetSpec  # type: ignore
    except ImportError:
        from confluent_kafka import OffsetSpec  # type: ignore
    from confluent_kafka import TopicPartition

    return OffsetSpec, TopicPartition


async def _resolve_partitions(
    admin: Any,
    topic_name: str,
    partitions: list[int] | None,
    loop: asyncio.AbstractEventLoop,
) -> list[int]:
    """If caller didn't specify partitions, fetch them from cluster metadata."""
    if partitions:
        return list(partitions)
    md = await loop.run_in_executor(None, lambda: admin.list_topics(timeout=15))
    topics = getattr(md, "topics", None) or {}
    if topic_name not in topics:
        raise MskToolError(
            ErrorType.INVALID_PARAMS,
            f"Topic not found: {topic_name}",
        )
    topic_md = topics[topic_name]
    parts = getattr(topic_md, "partitions", None) or {}
    return sorted(parts.keys())


def _summarize(topic: str, ts: int, total: int, not_found: int) -> str:
    if total == 0:
        return f"topic={topic} ts_ms={ts}: no partitions"
    if not_found == total:
        return (
            f"topic={topic} ts_ms={ts}: no messages found at or after this "
            "timestamp on any partition (it may be in the future)"
        )
    if not_found:
        return (
            f"topic={topic} ts_ms={ts}: offsets returned for "
            f"{total - not_found}/{total} partitions ({not_found} have no "
            "messages at/after this timestamp)"
        )
    return f"topic={topic} ts_ms={ts}: offsets returned for all {total} partitions"
