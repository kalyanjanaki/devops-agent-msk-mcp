from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.routing import Mount, Route

from msk_mcp.cli_executor import CliExecutor
from msk_mcp.client_properties import ClientPropertiesManager
from msk_mcp.concurrency import Bouncer
from msk_mcp.config import ClustersRegistry, Settings, load_registry, load_settings
from msk_mcp.http.health import health
from msk_mcp.kafka_clients import AdminClientFactory
from msk_mcp.logging_setup import CorrelationIdMiddleware, configure_logging
from msk_mcp.tools.compute_offset_reset_plan import (
    compute_offset_reset_plan as _compute_offset_reset_plan,
)
from msk_mcp.tools.describe_acls import describe_acls as _describe_acls
from msk_mcp.tools.describe_cluster import describe_cluster as _describe_cluster
from msk_mcp.tools.describe_consumer_group import (
    describe_consumer_group as _describe_consumer_group,
)
from msk_mcp.tools.describe_log_dirs import describe_log_dirs as _describe_log_dirs
from msk_mcp.tools.describe_partition_reassignments import (
    describe_partition_reassignments as _describe_partition_reassignments,
)
from msk_mcp.tools.describe_topic import describe_topic as _describe_topic
from msk_mcp.tools.describe_topic_configs import (
    describe_topic_configs as _describe_topic_configs,
)
from msk_mcp.tools.describe_under_replicated_partitions import (
    describe_under_replicated_partitions as _describe_under_replicated_partitions,
)
from msk_mcp.tools.get_offsets_for_times import get_offsets_for_times as _get_offsets_for_times
from msk_mcp.tools.list_consumer_groups import list_consumer_groups as _list_consumer_groups
from msk_mcp.tools.test_broker_connectivity import (
    probe_broker_connectivity as _probe_broker_connectivity,
)

logger = logging.getLogger(__name__)


@dataclass
class AppContext:
    settings: Settings
    registry: ClustersRegistry
    factory: AdminClientFactory
    properties: ClientPropertiesManager
    bouncer: Bouncer
    cli_executor: CliExecutor


def build_context(settings: Settings | None = None) -> AppContext:
    settings = settings or load_settings()
    configure_logging(settings.log_level)
    registry = load_registry(settings.clusters_config_path)
    factory = AdminClientFactory(registry)
    properties = ClientPropertiesManager(registry, settings.client_properties_dir)
    properties.render_all()
    bouncer = Bouncer(settings.tool_concurrency, settings.cli_concurrency)
    cli_executor = CliExecutor(bouncer.cli_semaphore, classpath=settings.kafka_classpath)
    return AppContext(settings, registry, factory, properties, bouncer, cli_executor)


def create_mcp(ctx: AppContext) -> FastMCP:
    mcp = FastMCP("msk-debug")

    @mcp.tool()
    async def list_consumer_groups(
        cluster_id: str,
        state_filter: str | None = None,
    ) -> dict[str, Any]:
        """List consumer groups on the given MSK cluster, optionally filtered by state."""
        return await ctx.bouncer.run_tool(
            _list_consumer_groups(
                factory=ctx.factory,
                cluster_id=cluster_id,
                state_filter=state_filter,
            ),
            timeout=ctx.settings.default_timeout_seconds,
        )

    @mcp.tool()
    async def describe_consumer_group(
        cluster_id: str,
        group_id: str,
        include_members: bool = True,
        include_offsets: bool = True,
    ) -> dict[str, Any]:
        """Describe a consumer group: state, members, offsets, lag.

        Use during rebalances when CloudWatch metrics go blind. Surfaces is_rebalancing,
        per-member partition assignments, and per-partition lag with the consumer host
        owning each partition.
        """
        return await ctx.bouncer.run_tool(
            _describe_consumer_group(
                factory=ctx.factory,
                cluster_id=cluster_id,
                group_id=group_id,
                include_members=include_members,
                include_offsets=include_offsets,
            ),
            timeout=ctx.settings.default_timeout_seconds,
        )

    @mcp.tool()
    async def describe_topic(
        cluster_id: str,
        topic_name: str,
    ) -> dict[str, Any]:
        """Describe a topic: partition layout, leader distribution, ISR membership."""
        return await ctx.bouncer.run_tool(
            _describe_topic(
                factory=ctx.factory,
                cluster_id=cluster_id,
                topic_name=topic_name,
            ),
            timeout=ctx.settings.default_timeout_seconds,
        )

    @mcp.tool()
    async def describe_cluster(cluster_id: str) -> dict[str, Any]:
        """Live cluster topology: brokers, controller ID, cluster UUID.

        Distinct from MSK control plane's view (which lags during incidents).
        The current controller ID is critical for any leader-election or
        controller-flapping investigation; neither CloudWatch nor MSK's API
        exposes it.
        """
        return await ctx.bouncer.run_tool(
            _describe_cluster(factory=ctx.factory, cluster_id=cluster_id),
            timeout=ctx.settings.default_timeout_seconds,
        )

    @mcp.tool()
    async def describe_under_replicated_partitions(
        cluster_id: str,
        topic_filter: str | None = None,
    ) -> dict[str, Any]:
        """Find all under-replicated partitions on the cluster, with broker attribution.

        For each affected partition returns missing_from_isr (which brokers dropped out),
        plus broker_drop_counts so the agent can spot a single broker that's consistently
        the cause across many partitions. Use during ISR-shrinkage incidents.
        """
        return await ctx.bouncer.run_tool(
            _describe_under_replicated_partitions(
                factory=ctx.factory,
                cluster_id=cluster_id,
                topic_filter=topic_filter,
            ),
            timeout=ctx.settings.default_timeout_seconds,
        )

    @mcp.tool()
    async def describe_acls(
        cluster_id: str,
        resource_type: str | None = None,
        resource_name: str | None = None,
        principal: str | None = None,
    ) -> dict[str, Any]:
        """List Kafka ACLs (filtered by resource_type/name/principal if provided).

        On clusters using IAM auth, permissions are managed via IAM policies and
        Kafka ACLs are typically empty — the summary calls this out so the agent
        doesn't waste cycles concluding ACLs are 'missing'.
        """
        return await ctx.bouncer.run_tool(
            _describe_acls(
                factory=ctx.factory,
                registry=ctx.registry,
                cluster_id=cluster_id,
                resource_type=resource_type,
                resource_name=resource_name,
                principal=principal,
            ),
            timeout=ctx.settings.default_timeout_seconds,
        )

    @mcp.tool()
    async def get_offsets_for_times(
        cluster_id: str,
        topic_name: str,
        timestamp_ms: int,
        partitions: list[int] | None = None,
    ) -> dict[str, Any]:
        """Find each partition's offset at a specific point in time.

        Given epoch_ms (e.g. when an alarm fired), returns the offset of the
        first message with timestamp >= timestamp_ms on each partition. Used
        during incident timeline reconstruction: 'how far behind was the
        consumer when the alarm fired?', 'what's the first message we need
        to reprocess?'.
        """
        return await ctx.bouncer.run_tool(
            _get_offsets_for_times(
                factory=ctx.factory,
                cluster_id=cluster_id,
                topic_name=topic_name,
                timestamp_ms=timestamp_ms,
                partitions=partitions,
            ),
            timeout=ctx.settings.default_timeout_seconds,
        )

    @mcp.tool()
    async def describe_partition_reassignments(
        cluster_id: str,
        topic_filter: str | None = None,
    ) -> dict[str, Any]:
        """In-progress partition reassignments as the controller sees them.

        Direct view of the dedicated Kafka API. For each in-progress
        reassignment returns adding_replicas / removing_replicas so the agent
        can identify which broker pairs are moving data and spot reassignments
        that have been queued forever (stuck).
        """
        return await ctx.bouncer.run_tool(
            _describe_partition_reassignments(
                factory=ctx.factory,
                cluster_id=cluster_id,
                topic_filter=topic_filter,
            ),
            timeout=ctx.settings.default_timeout_seconds,
        )

    @mcp.tool()
    async def describe_topic_configs(
        cluster_id: str,
        topic_name: str,
    ) -> dict[str, Any]:
        """Topic-level configuration as the broker sees it right now.

        Returns each config with its source (DYNAMIC_TOPIC_CONFIG vs DEFAULT_CONFIG
        vs STATIC_BROKER_CONFIG) — the field MSK's control-plane Topic API doesn't
        expose. Surfaces notable_overrides for compression.type, cleanup.policy,
        min.insync.replicas etc. — the configs that most often cause silent
        perf/correctness issues.
        """
        return await ctx.bouncer.run_tool(
            _describe_topic_configs(
                factory=ctx.factory,
                cluster_id=cluster_id,
                topic_name=topic_name,
            ),
            timeout=ctx.settings.default_timeout_seconds,
        )

    @mcp.tool()
    async def test_broker_connectivity(
        cluster_id: str,
        broker_endpoint: str,
    ) -> dict[str, Any]:
        """Probe a single broker endpoint and pinpoint the failure stage.

        Returns failure_stage = NETWORK | TLS | SASL | PROTOCOL | None on success.
        Use to differentiate 'security group blocks me' from 'IAM policy is wrong'
        from 'broker version too old' — answers each in one call.
        """
        return await ctx.bouncer.run_tool(
            _probe_broker_connectivity(
                registry=ctx.registry,
                cluster_id=cluster_id,
                broker_endpoint=broker_endpoint,
                timeout=ctx.settings.default_timeout_seconds,
            ),
            timeout=ctx.settings.default_timeout_seconds + 5.0,
        )

    @mcp.tool()
    async def compute_offset_reset_plan(
        cluster_id: str,
        group_id: str,
        topic_name: str,
        reset_strategy: str,
        offset_value: int | None = None,
    ) -> dict[str, Any]:
        """Compute what a consumer-group offset reset WOULD do, without applying it.

        Always runs `kafka-consumer-groups.sh --reset-offsets ... --dry-run` —
        this tool NEVER mutates broker state. Returns the per-partition
        before/after offsets and a `remediation_command` string a human can
        copy-paste with `--execute` to actually apply the change.

        reset_strategy: "to-latest" | "to-earliest" | "to-offset" | "shift-by".
        offset_value is required for "to-offset" and "shift-by".
        """
        return await ctx.bouncer.run_tool(
            _compute_offset_reset_plan(
                registry=ctx.registry,
                properties=ctx.properties,
                executor=ctx.cli_executor,
                kafka_bin_path=str(ctx.settings.kafka_bin_path),
                timeout=ctx.settings.default_timeout_seconds,
                cluster_id=cluster_id,
                group_id=group_id,
                topic_name=topic_name,
                reset_strategy=reset_strategy,
                offset_value=offset_value,
            ),
            timeout=ctx.settings.default_timeout_seconds + 5.0,
        )

    @mcp.tool()
    async def describe_log_dirs(
        cluster_id: str,
        broker_ids: str | None = None,
        topic_filter: str | None = None,
    ) -> dict[str, Any]:
        """Per-broker log dir sizes, stuck reassignments (isFuture=true), and disk errors.

        Runs kafka-log-dirs.sh under the hood. Surfaces stuck_reassignments and
        disk_errors prominently so the agent doesn't need to scan the full payload.
        """
        return await ctx.bouncer.run_tool(
            _describe_log_dirs(
                registry=ctx.registry,
                properties=ctx.properties,
                executor=ctx.cli_executor,
                kafka_bin_path=str(ctx.settings.kafka_bin_path),
                timeout=ctx.settings.default_timeout_seconds,
                cluster_id=cluster_id,
                broker_ids=broker_ids,
                topic_filter=topic_filter,
            ),
            timeout=ctx.settings.default_timeout_seconds + 5.0,
        )

    return mcp


def build_app(ctx: AppContext | None = None) -> Starlette:
    """Compose the Starlette ASGI app: MCP transport + /health."""
    ctx = ctx or build_context()
    mcp = create_mcp(ctx)
    mcp_app = mcp.streamable_http_app()
    app = Starlette(
        routes=[
            Route("/health", health, methods=["GET"]),
            Mount("/", app=mcp_app),
        ],
        lifespan=mcp_app.router.lifespan_context,
    )
    app.add_middleware(CorrelationIdMiddleware)
    return app
