from __future__ import annotations

import asyncio
from typing import Any

from msk_mcp.errors import ErrorType, MskToolError, tool_error_handler
from msk_mcp.kafka_clients import AdminClientFactory

_REBALANCING_STATES = {"PREPARINGREBALANCE", "COMPLETINGREBALANCE"}


@tool_error_handler
async def describe_consumer_group(
    *,
    factory: AdminClientFactory,
    cluster_id: str,
    group_id: str,
    include_members: bool = True,
    include_offsets: bool = True,
) -> dict[str, Any]:
    """Describe a single consumer group: state, members, offsets, lag.

    The most diagnostically important tool: it answers questions CloudWatch can't
    during rebalances (which member is stuck, what's the per-partition lag).
    """
    admin = factory.get(cluster_id)
    loop = asyncio.get_running_loop()

    desc_map = await loop.run_in_executor(None, lambda: admin.describe_consumer_groups([group_id]))
    if group_id not in desc_map:
        raise MskToolError(
            ErrorType.INVALID_PARAMS,
            f"Consumer group not found: {group_id}",
            suggestion="Use list_consumer_groups to enumerate available groups.",
        )

    desc = await loop.run_in_executor(None, lambda: desc_map[group_id].result())
    state = _state_name(getattr(desc, "state", None))

    members_out: list[dict[str, Any]] = []
    if include_members:
        for m in getattr(desc, "members", []) or []:
            assigned: list[dict[str, Any]] = []
            assignment = getattr(m, "assignment", None)
            tps = getattr(assignment, "topic_partitions", None) if assignment else None
            if tps:
                for tp in tps:
                    assigned.append({"topic": tp.topic, "partition": tp.partition})
            members_out.append(
                {
                    "consumer_id": getattr(m, "member_id", None),
                    "host": getattr(m, "host", None),
                    "client_id": getattr(m, "client_id", None),
                    "assigned_partitions": assigned,
                }
            )

    offsets_out: list[dict[str, Any]] = []
    if include_offsets:
        offsets_out = await _fetch_offsets(admin, group_id, members_out, loop)

    return {
        "cluster_id": cluster_id,
        "group_id": group_id,
        "state": state,
        "protocol_type": getattr(desc, "protocol_type", None),
        "coordinator": _coordinator_id(getattr(desc, "coordinator", None)),
        "members": members_out,
        "offsets": offsets_out,
        "is_rebalancing": (state or "").upper() in _REBALANCING_STATES,
        "summary": _summarize(state, len(members_out), len(offsets_out)),
    }


def _state_name(state_obj: Any) -> str | None:
    if state_obj is None:
        return None
    return getattr(state_obj, "name", None) or str(state_obj)


def _coordinator_id(node: Any) -> int | None:
    if node is None:
        return None
    return getattr(node, "id", None)


def _summarize(state: str | None, member_count: int, offset_count: int) -> str:
    return f"state={state}, members={member_count}, offsets_partitions={offset_count}"


async def _fetch_offsets(
    admin: Any,
    group_id: str,
    members_out: list[dict[str, Any]],
    loop: asyncio.AbstractEventLoop,
) -> list[dict[str, Any]]:
    """Fetch committed offsets, then look up log-end offsets per partition to derive lag.

    Built defensively: confluent-kafka's exact API names for these calls have shifted
    across versions, so we use whichever is available and fall back gracefully.
    """
    try:
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    except ImportError:  # pragma: no cover
        return []

    request = ConsumerGroupTopicPartitions(group_id, None)
    futures = await loop.run_in_executor(
        None, lambda: admin.list_consumer_group_offsets([request])
    )
    if group_id not in futures:
        return []
    cgtps = await loop.run_in_executor(None, lambda: futures[group_id].result())
    committed = list(getattr(cgtps, "topic_partitions", []) or [])
    if not committed:
        return []

    end_offsets = await _fetch_end_offsets(admin, committed, loop, TopicPartition)
    member_lookup = _members_by_partition(members_out)

    out: list[dict[str, Any]] = []
    for tp in committed:
        key = (tp.topic, tp.partition)
        end = end_offsets.get(key)
        current = tp.offset if tp.offset >= 0 else None
        lag = (end - current) if (end is not None and current is not None) else None
        m = member_lookup.get(key, {})
        out.append(
            {
                "topic": tp.topic,
                "partition": tp.partition,
                "current_offset": current,
                "log_end_offset": end,
                "lag": lag,
                "consumer_id": m.get("consumer_id"),
                "host": m.get("host"),
                "client_id": m.get("client_id"),
            }
        )
    return out


async def _fetch_end_offsets(
    admin: Any,
    tps: list[Any],
    loop: asyncio.AbstractEventLoop,
    TopicPartition: Any,
) -> dict[tuple[str, int], int]:
    """Returns {(topic, partition): log_end_offset}. Uses list_offsets if available."""
    list_offsets = getattr(admin, "list_offsets", None)
    if not list_offsets:
        return {}

    try:
        try:
            from confluent_kafka.admin import OffsetSpec  # type: ignore
        except ImportError:
            from confluent_kafka import OffsetSpec  # type: ignore
        spec = OffsetSpec.latest()  # type: ignore[attr-defined]
        request = {TopicPartition(tp.topic, tp.partition): spec for tp in tps}
    except Exception:
        return {}

    try:
        result_map = await loop.run_in_executor(None, lambda: admin.list_offsets(request))
    except Exception:
        return {}

    out: dict[tuple[str, int], int] = {}
    for tp, fut in result_map.items():
        try:
            res = await loop.run_in_executor(None, lambda f=fut: f.result())
            offset = getattr(res, "offset", None)
            if offset is not None:
                out[(tp.topic, tp.partition)] = offset
        except Exception:
            continue
    return out


def _members_by_partition(members: list[dict[str, Any]]) -> dict[tuple[str, int], dict[str, Any]]:
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for m in members:
        for tp in m.get("assigned_partitions", []):
            out[(tp["topic"], tp["partition"])] = m
    return out
