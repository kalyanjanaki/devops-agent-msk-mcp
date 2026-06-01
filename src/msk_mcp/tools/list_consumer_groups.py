from __future__ import annotations

import asyncio
from typing import Any

from msk_mcp.errors import tool_error_handler
from msk_mcp.kafka_clients import AdminClientFactory


@tool_error_handler
async def list_consumer_groups(
    *,
    factory: AdminClientFactory,
    cluster_id: str,
    state_filter: str | None = None,
) -> dict[str, Any]:
    """List consumer groups on the given MSK cluster, optionally filtered by state."""
    admin = factory.get(cluster_id)
    loop = asyncio.get_running_loop()
    listing = await loop.run_in_executor(
        None,
        lambda: admin.list_consumer_groups(request_timeout=15).result(),
    )

    groups: list[dict[str, Any]] = []
    for g in listing.valid:
        state_obj = getattr(g, "state", None)
        if state_obj is None:
            state = None
        else:
            state = getattr(state_obj, "name", None) or str(state_obj)
        groups.append({"group_id": g.group_id, "state": state})

    if state_filter:
        sf = state_filter.upper()
        groups = [g for g in groups if (g["state"] or "").upper() == sf]

    raw_errors = getattr(listing, "errors", None) or []
    errors = [str(err) for err in raw_errors]

    return {
        "cluster_id": cluster_id,
        "consumer_groups": [g["group_id"] for g in groups],
        "groups_with_state": groups,
        "total_count": len(groups),
        "errors": errors,
        "summary": _summarize(len(groups), groups, state_filter, len(errors)),
    }


def _summarize(
    total: int,
    groups: list[dict[str, Any]],
    state_filter: str | None,
    error_count: int,
) -> str:
    if total == 0:
        if state_filter:
            return f"No consumer groups in state {state_filter.upper()}"
        return "No consumer groups on this cluster"
    state_counts: dict[str, int] = {}
    for g in groups:
        key = g["state"] or "UNKNOWN"
        state_counts[key] = state_counts.get(key, 0) + 1
    breakdown = ", ".join(f"{c} {s}" for s, c in sorted(state_counts.items()))
    base = f"{total} group(s): {breakdown}"
    if state_filter:
        base = f"{total} group(s) in state {state_filter.upper()}: {breakdown}"
    if error_count:
        base += f"; {error_count} listing error(s)"
    return base
