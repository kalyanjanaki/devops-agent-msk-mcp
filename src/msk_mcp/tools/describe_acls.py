from __future__ import annotations

import asyncio
from typing import Any

from msk_mcp.config import ClustersRegistry, IamCluster
from msk_mcp.errors import tool_error_handler
from msk_mcp.kafka_clients import AdminClientFactory


@tool_error_handler
async def describe_acls(
    *,
    factory: AdminClientFactory,
    registry: ClustersRegistry,
    cluster_id: str,
    resource_type: str | None = None,
    resource_name: str | None = None,
    principal: str | None = None,
) -> dict[str, Any]:
    """List Kafka ACLs (filtered by resource_type/name/principal if provided).

    NOTE: MSK clusters with IAM auth enforce permissions via IAM policies, not
    Kafka ACLs. On those clusters this tool typically returns an empty list —
    that's not a bug, the auth model is different. The summary calls this out
    so the agent doesn't waste cycles thinking ACLs are 'missing'.
    """
    cfg = registry.get(cluster_id)
    is_iam = isinstance(cfg, IamCluster)

    admin = factory.get(cluster_id)
    loop = asyncio.get_running_loop()

    AclBindingFilter, ResourceType, ResourcePatternType, AclOperation, AclPermissionType = (
        _import_acl_types()
    )

    rtype = _resolve_resource_type(resource_type, ResourceType)
    rname = resource_name  # None means ANY
    rpattern = ResourcePatternType.ANY
    op = AclOperation.ANY
    perm = AclPermissionType.ANY
    principal_filter = principal  # None means ANY
    host_filter = None  # ANY host

    acl_filter = AclBindingFilter(
        rtype, rname, rpattern, principal_filter, host_filter, op, perm
    )

    fut_or_obj = await loop.run_in_executor(
        None, lambda: admin.describe_acls(acl_filter)
    )
    bindings = await _resolve(fut_or_obj, loop)

    out: list[dict[str, Any]] = []
    for b in bindings or []:
        out.append(
            {
                "resource_type": _enum_name(getattr(b, "restype", None)),
                "resource_name": getattr(b, "name", None),
                "pattern_type": _enum_name(getattr(b, "resource_pattern_type", None)),
                "principal": getattr(b, "principal", None),
                "host": getattr(b, "host", None),
                "operation": _enum_name(getattr(b, "operation", None)),
                "permission": _enum_name(getattr(b, "permission_type", None)),
            }
        )

    return {
        "cluster_id": cluster_id,
        "filter": {
            "resource_type": resource_type,
            "resource_name": resource_name,
            "principal": principal,
        },
        "acls": out,
        "total_count": len(out),
        "summary": _summarize(len(out), is_iam),
    }


def _import_acl_types():
    from confluent_kafka.admin import (
        AclBindingFilter,
        AclOperation,
        AclPermissionType,
        ResourcePatternType,
        ResourceType,
    )
    return AclBindingFilter, ResourceType, ResourcePatternType, AclOperation, AclPermissionType


def _resolve_resource_type(resource_type: str | None, ResourceType: Any) -> Any:
    if resource_type is None:
        return ResourceType.ANY
    name = resource_type.upper()
    return getattr(ResourceType, name, ResourceType.ANY)


async def _resolve(fut_or_obj: Any, loop: asyncio.AbstractEventLoop) -> Any:
    if hasattr(fut_or_obj, "result"):
        return await loop.run_in_executor(None, lambda: fut_or_obj.result())
    return fut_or_obj


def _enum_name(v: Any) -> str | None:
    if v is None:
        return None
    return getattr(v, "name", None) or str(v)


def _summarize(total: int, is_iam: bool) -> str:
    if total == 0 and is_iam:
        return (
            "No Kafka ACLs (cluster uses IAM auth — permissions are managed "
            "via IAM policies, not Kafka ACLs)."
        )
    if total == 0:
        return "No Kafka ACLs match the filter"
    return f"{total} ACL binding(s) match the filter"
