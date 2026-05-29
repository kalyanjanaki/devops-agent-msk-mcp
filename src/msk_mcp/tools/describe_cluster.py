from __future__ import annotations

import asyncio
from typing import Any

from msk_mcp.errors import tool_error_handler
from msk_mcp.kafka_clients import AdminClientFactory


@tool_error_handler
async def describe_cluster(
    *,
    factory: AdminClientFactory,
    cluster_id: str,
) -> dict[str, Any]:
    """Live cluster topology: brokers, controller ID, cluster UUID.

    Distinct from MSK control plane's view, which lags during incidents.
    The current controller ID is the single most useful piece of info during
    leader-election storms — neither CloudWatch nor MSK's API exposes it.
    """
    admin = factory.get(cluster_id)
    loop = asyncio.get_running_loop()

    desc = await _describe(admin, loop)

    nodes = list(getattr(desc, "nodes", []) or [])
    brokers = [
        {
            "broker_id": getattr(n, "id", None),
            "host": getattr(n, "host", None),
            "port": getattr(n, "port", None),
            "rack": getattr(n, "rack", None),
        }
        for n in nodes
    ]

    controller = getattr(desc, "controller", None)
    controller_id = getattr(controller, "id", None) if controller else None

    cluster_uuid = (
        getattr(desc, "cluster_id", None)
        or getattr(desc, "id", None)
        or None
    )

    return {
        "cluster_id": cluster_id,
        "cluster_uuid": cluster_uuid,
        "controller_id": controller_id,
        "broker_count": len(brokers),
        "brokers": sorted(brokers, key=lambda b: b["broker_id"] or 0),
        "summary": _summarize(controller_id, len(brokers), cluster_uuid),
    }


async def _describe(admin: Any, loop: asyncio.AbstractEventLoop) -> Any:
    """describe_cluster() returns a future-like; result() blocks until ready."""
    fut_or_obj = await loop.run_in_executor(None, lambda: admin.describe_cluster())
    # Newer SDK returns a future; older returns the object directly. Handle both.
    if hasattr(fut_or_obj, "result"):
        return await loop.run_in_executor(None, lambda: fut_or_obj.result())
    return fut_or_obj


def _summarize(controller_id: int | None, broker_count: int, cluster_uuid: str | None) -> str:
    parts = [f"controller=broker_{controller_id}", f"brokers={broker_count}"]
    if cluster_uuid:
        parts.append(f"cluster_uuid={cluster_uuid}")
    return ", ".join(parts)
