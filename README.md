# MSK Debugging MCP Server

Read-only MCP server that exposes Amazon MSK debugging primitives to AWS DevOps Agent over Streamable HTTP. Runs as an ECS Fargate task behind a VPC Lattice service.

## The problem we're solving

AWS DevOps Agent does not have direct access to customer MSK clusters. Out of the box, it can only see what the AWS control plane exposes — CloudWatch metrics, CloudWatch Logs, MSK Topic API, broker info from `kafka:DescribeCluster`. That's enough to recognize *that* something is wrong, but not enough to diagnose *why* most of the time, so the agent ends up either guessing from metrics or telling the human "you'll need to SSH to a bastion and run `kafka-consumer-groups.sh` yourself."

This MCP server gives the agent a safe, read-only path **into the broker itself**: live metadata, consumer-group state, ISR membership, log-dir contents, partition reassignments, broker connectivity probes, and so on. The agent can now answer questions that previously required a human and a CLI session.

## Why this exists (the technical gap)

Amazon MSK's monitoring stack — CloudWatch metrics, Prometheus, MSK Topic API, control-plane APIs — can detect *that* problems exist but can't always reveal *why*. Concrete blind spots this server fills:

- CloudWatch consumer lag metrics **vanish during rebalance storms** (only emitted in STABLE/EMPTY state).
- No metric identifies **which specific consumer instance owns a lagging partition**.
- No metric shows **per-partition ISR membership** or which broker dropped from ISR.
- No metric reveals **stuck partition reassignments** or disk-level errors.
- The MSK control plane's view of topic config can **lag the live broker view**, and doesn't expose the `config_source` field needed to distinguish a topic-level override from an inherited default.
- No metric exposes the **current Kafka controller broker ID** — the load-bearing fact during leader-election storms.

The 12 tools in this server answer those questions by talking directly to brokers via the Kafka AdminClient (Python `confluent-kafka` library, a thin binding over librdkafka) or by shelling out to the official Kafka CLI for the few operations the AdminClient doesn't expose cleanly.

## Strictly read-only

No tool mutates Kafka state. Where a debugging path naturally leads to a remediation step (e.g., resetting a consumer group's offsets), the relevant tool returns the proposed change as structured JSON **and** the exact CLI command a human would run to apply it. The server itself never executes the mutation.

This means the server's IAM/SCRAM/mTLS credentials can be scoped to read-only Kafka actions, eliminating a whole class of risk.

## Architecture

- **Execution**: Hybrid — Python AdminClient (`confluent-kafka` over librdkafka) for fast metadata calls; Kafka CLI subprocess (`kafka-log-dirs.sh`, `kafka-consumer-groups.sh --dry-run`) for the two operations where the AdminClient surface is awkward or absent.
- **MCP SDK**: FastMCP with `transport="streamable-http"`.
- **Auth from DevOps Agent → MCP**: VPC Lattice SigV4 at the network layer. The MCP server is plain HTTP behind Lattice; no app-layer auth needed.
- **Auth from MCP → MSK**: IAM (live), SASL/SCRAM (stubbed), mTLS (stubbed). Per-cluster in `clusters.yaml`.
- **Multi-cluster**: One server can serve N clusters via a registry; every tool takes a `cluster_id` parameter.
- **Concurrency**: Two-tier asyncio bouncer — overall tool-call limit (default 16), stricter inner limit on CLI subprocesses (default 4) to cap JVM fan-out.
- **Errors**: Structured taxonomy returned to the agent — `AUTH_FAILURE`, `NETWORK_TIMEOUT`, `AUTHORIZATION`, `INVALID_PARAMS`, `EXECUTION_FAILURE`, `TIMEOUT` — each with a `suggestion` field.
- **Logging**: structlog JSON, with a per-request correlation ID propagated into subprocess env (`MSK_MCP_CID`) for tracing across the Python/JVM boundary.

## Tools

All tools take `cluster_id` (matching a key in `clusters.yaml`) as the first argument. They return JSON envelopes; on error they return `{ "error": true, "error_type": "...", "error_message": "...", "suggestion": "..." }` instead of raising.

### Server-side diagnostics

#### `describe_cluster`
Live cluster topology as the brokers see it: broker list (id, host, port, rack), **current controller broker ID**, and the Kafka cluster UUID.

- **Why use it**: The controller ID is the single most useful piece of info during leader-election or controller-flapping investigations. Neither CloudWatch nor MSK's control-plane API exposes it. The broker list also catches drift between MSK's declared inventory and the cluster's live membership (e.g., a broker process that's crashed but the EC2/Fargate task is still up).
- **Typical issues it debugs**: controller flapping or instability; leader-election storms; brokers crashed at the process level but still listed as "ACTIVE" in MSK; verifying which AZs are represented after a rebalance.
- **Args**: `cluster_id`

#### `describe_topic`
Partition layout, leader distribution, and ISR membership for a single topic.

- **Why use it**: First call when a producer reports issues with a specific topic — confirms partition count, replication factor, leader-broker distribution, and which partitions (if any) are under-replicated.
- **Typical issues it debugs**: producer "leader not available" errors on a specific topic; uneven leader distribution (one broker hosting most partitions and getting hot); replication-factor mismatches between what was provisioned and what the brokers actually have.
- **Args**: `cluster_id`, `topic_name`

#### `describe_under_replicated_partitions`
Scans all topics on the cluster and reports partitions where `len(isr) < len(replicas)`. Returns each affected partition's `missing_from_isr` list, plus a `broker_drop_counts` histogram across all under-replicated partitions.

- **Why use it**: Answers "is broker N consistently dropping out?" in one call. The histogram makes a single misbehaving broker pop out immediately. Tolerates per-topic ACL denies — those topics are skipped, not failed.
- **Typical issues it debugs**: `UnderReplicatedPartitions` CloudWatch alarm firing without telling you which broker is the cause; replication lag after a broker restart; one broker silently falling behind due to disk pressure or network saturation.
- **Args**: `cluster_id`, optional `topic_filter`

#### `describe_log_dirs`
Per-broker log directory sizes, with `is_future` (stuck reassignment) and per-log-dir `error` (disk failure) fields surfaced prominently.

- **Why use it**: Detects size-based partition skew, stuck partition moves (`isFuture: true` + non-decreasing `offsetLag`), and disk-level failures (`KafkaStorageException`) that don't surface as Kafka-level errors. This is the only tool that goes through the CLI subprocess path.
- **Typical issues it debugs**: `KafkaStorageException` after a disk fault; partition reassignment that ran for hours and never finished; one broker filling up disproportionately because of leader skew; mysterious producer back-pressure that turns out to be a single broker running out of disk.
- **Args**: `cluster_id`, optional `broker_ids` (comma-separated), optional `topic_filter`

#### `describe_partition_reassignments`
In-progress partition reassignments as the controller sees them: `adding_replicas`, `removing_replicas`, `current_replicas` per partition.

- **Why use it**: More authoritative than inferring from `describe_log_dirs.is_future`. (Note: the underlying AdminClient method `list_partition_reassignments` isn't in confluent-kafka 2.14 yet; the tool returns a clean empty response with a summary pointing to `describe_log_dirs` until that lands. Future SDK upgrade unlocks the direct path.)
- **Typical issues it debugs**: a `kafka-reassign-partitions.sh` job a human kicked off hours ago that never completed; figuring out which broker pair is actively moving data when the cluster is under unusual load; confirming a reassignment finished cleanly before another one is started.
- **Args**: `cluster_id`, optional `topic_filter`

### Client-side / consumer diagnostics

#### `list_consumer_groups`
List of consumer groups on the cluster, optionally filtered by state.

- **Why use it**: Discovery — answer "what groups exist on this cluster?" before drilling into one.
- **Typical issues it debugs**: a customer asks about "the lag on our orders pipeline" without naming the actual group; spotting unexpected groups left behind by old applications; finding all groups currently in `PREPARING_REBALANCING` during an incident.
- **Args**: `cluster_id`, optional `state_filter` (e.g. `STABLE`, `EMPTY`, `PREPARING_REBALANCING`)

#### `describe_consumer_group`
The most diagnostic tool. Returns group state, members (with consumer host IPs and client IDs), per-member partition assignments, and per-partition `current_offset` / `log_end_offset` / `lag`. Includes an `is_rebalancing` boolean.

- **Why use it**: Fills the monitoring blind spot where CloudWatch goes blind during rebalances. Surfaces which specific consumer instance is stuck (frozen `current_offset` while `log_end_offset` advances), which host owns each lagging partition, and the live group state.
- **Typical issues it debugs**: a rebalance storm where CloudWatch lag metrics disappear; one stuck consumer instance holding up an entire partition's progress; "consumer X is mis-deployed" — confirmed by spotting an old client_id still in the assignment; lag concentrated on specific partitions rather than spread evenly (often a hot-key issue downstream).
- **Args**: `cluster_id`, `group_id`, optional `include_members` (default true), optional `include_offsets` (default true)

#### `get_offsets_for_times`
Given a timestamp (epoch ms) and a topic, returns the offset of the first message at or after that timestamp on each partition. Returns `offset = -1, found = false` for partitions with no message at/after the requested time.

- **Why use it**: Incident timeline reconstruction. "How far behind was the consumer when the alarm fired at 09:00 UTC?" "What's the first message we'd need to reprocess?"
- **Typical issues it debugs**: post-incident replay — "we want to reprocess everything since 14:00 UTC, what offsets is that on each partition?"; deciding whether a consumer's lag at the time of the alarm was already bad before the incident or only grew during it; identifying the message offset where a known bad event happened.
- **Args**: `cluster_id`, `topic_name`, `timestamp_ms`, optional `partitions` (list of ints)

### Configuration & connectivity diagnostics

#### `describe_topic_configs`
Topic-level configuration **as the broker sees it right now**, with each config's `source` resolved to its enum name (`DYNAMIC_TOPIC_CONFIG` vs `STATIC_BROKER_CONFIG` vs `DEFAULT_CONFIG`). Surfaces a `notable_overrides` list for high-impact configs (`compression.type`, `cleanup.policy`, `min.insync.replicas`, `retention.ms`, `unclean.leader.election.enable`, etc.).

- **Why use it**: The flagship use case is producer/topic compression-codec mismatches (silent CPU killer — broker decompresses and recompresses every batch). The `source` field is what answers "is this an override or inherited?", and it's not exposed by MSK's control-plane Topic API. Topics created via raw Kafka admin clients (Terraform/CDK with direct admin calls, app code) may not appear in MSK's Topic API at all — this tool always sees them.
- **Typical issues it debugs**: producer using `compression.type=lz4` against a topic configured for `snappy` (broker CPU spike + throughput drop); a topic that is "supposed to" have 7-day retention but is silently using broker default 24-hour retention; `min.insync.replicas` set to 1 in violation of policy; topics created out-of-band that don't show up in MSK's Topic API.
- **Args**: `cluster_id`, `topic_name`

#### `test_broker_connectivity`
Targeted probe of a single broker endpoint with explicit failure-stage classification: `NETWORK` | `TLS` | `SASL` | `PROTOCOL` | `null` (success).

- **Why use it**: Differentiates "security group blocks me" from "my IAM policy is wrong" from "broker version too old to speak my client's wire protocol". TCP probe runs first for fast triage; on success, hands off to a single-bootstrap AdminClient for the full handshake.
- **Typical issues it debugs**: customer reports "I can't connect to MSK" — was it the security group, missing route, expired IAM token, or wrong port?; one broker reachable but another isn't (security-group rule scoped wrongly); intermittent connection failures that turn out to be TLS-handshake timeouts; producer/consumer using a port the cluster doesn't actually listen on.
- **Args**: `cluster_id`, `broker_endpoint` (e.g. `b-1.cluster.kafka.us-east-1.amazonaws.com:9098`)

#### `describe_acls`
List Kafka ACLs with optional `resource_type` / `resource_name` / `principal` filters.

- **Why use it**: Authorization-failure triage on clusters that use Kafka ACLs. On clusters using IAM auth (where permissions live in IAM policies, not Kafka ACLs), the result is typically empty — the summary explicitly calls this out so the agent doesn't conclude ACLs are 'missing'.
- **Typical issues it debugs**: `TopicAuthorizationException` after authentication succeeded — the principal is missing READ/WRITE/DESCRIBE on the resource; a recently rotated principal whose ACLs weren't migrated; an ACL inadvertently scoped to `LITERAL` when the resource name expected `PREFIXED`.
- **Args**: `cluster_id`, optional `resource_type` (`TOPIC`, `GROUP`, `CLUSTER`, etc.), optional `resource_name`, optional `principal`

### Remediation planning (read-only)

#### `compute_offset_reset_plan`
Computes what a consumer-group offset reset *would* do, without applying it. Always passes `--dry-run` to `kafka-consumer-groups.sh --reset-offsets`. Returns the proposed per-partition `new_offset`, plus a `remediation_command` string a human can copy-paste with `--execute` to actually apply the change.

- **Why use it**: Lets the agent confidently answer "if we reset this group to earliest, what happens?" without any mutation risk. The MCP server itself never runs the `--execute` form. The output schema always sets `dry_run: true` — a contract test enforces that.
- **Typical issues it debugs**: a consumer is wedged on a poison pill and the team needs to skip past it — what offset would `--shift-by N` actually land on per partition?; planning a "replay everything from yesterday" reset and validating the proposed new offsets before any human runs `--execute`; sanity-checking a remediation a human is about to perform during an incident.
- **Args**: `cluster_id`, `group_id`, `topic_name`, `reset_strategy` (`to-latest` | `to-earliest` | `to-offset` | `shift-by`), optional `offset_value` (required for `to-offset` and `shift-by`)

## Local development

```sh
uv sync --extra dev
cp config/clusters.yaml.example config/clusters.yaml
# Edit clusters.yaml with a dev cluster's bootstrap servers and auth_type
./scripts/run_local.sh
```

The server listens on `http://localhost:8080/mcp`. Hit it with `curl`, the official MCP Python client, or the MCP Inspector.

If you have a local Kafka CLI installed in a non-standard location:

```sh
export MSK_MCP_KAFKA_BIN_PATH=$HOME/kafka_2.13-3.8.0/bin
export MSK_MCP_KAFKA_CLASSPATH=$HOME/kafka-libs/aws-msk-iam-auth-1.1.9-all.jar
./scripts/run_local.sh
```

## Run tests

```sh
uv run pytest
```

132 tests covering unit (config, errors, concurrency, CLI executor, client.properties, logging) and tool integration (mocked AdminClient + mocked subprocess + in-process FastMCP wiring).

## Build & run container

```sh
./scripts/build_image.sh                # uses docker if available, else finch
finch run -d --name msk-mcp \
  -p 8080:8080 \
  -e AWS_REGION=us-east-1 \
  --env-file /tmp/aws-creds.env \
  -v $(pwd)/config/clusters.yaml:/etc/msk-mcp/clusters.yaml:ro \
  msk-mcp:dev
```

The image is `linux/amd64` by default (override with `PLATFORM=linux/arm64` for Graviton Fargate). It bundles Python 3.11, Eclipse Temurin JRE 17, Kafka 3.9.x CLI, and the `aws-msk-iam-auth` JAR. Runs as non-root user `mskmcp`.

## Configuration

### Server-level env vars

| Env var | Default | Notes |
|---|---|---|
| `MSK_MCP_CLUSTERS_CONFIG_PATH` | `/etc/msk-mcp/clusters.yaml` | Cluster registry file |
| `MSK_MCP_PORT` | `8080` | HTTP port |
| `MSK_MCP_TOOL_CONCURRENCY` | `16` | Max concurrent tool calls |
| `MSK_MCP_CLI_CONCURRENCY` | `4` | Max concurrent CLI subprocesses (JVM fan-out cap) |
| `MSK_MCP_DEFAULT_TIMEOUT_SECONDS` | `30` | Per-tool timeout |
| `MSK_MCP_LOG_LEVEL` | `INFO` |  |
| `MSK_MCP_KAFKA_BIN_PATH` | `/opt/kafka/bin` | Kafka CLI directory |
| `MSK_MCP_KAFKA_CLASSPATH` | (unset) | Extra CLASSPATH for the CLI subprocess (e.g. local IAM JAR path) |
| `MSK_MCP_CLIENT_PROPERTIES_DIR` | `/tmp/msk-mcp` | Where rendered `client.properties` files go |

### Per-cluster registry (`clusters.yaml`)

```yaml
clusters:
  prod-east:
    bootstrap_servers: b-1.prod-east.cluster.kafka.us-east-1.amazonaws.com:9098,b-2...:9098
    region: us-east-1
    auth_type: IAM
    description: "Customer prod cluster, IAM auth"

  legacy-scram:
    bootstrap_servers: b-1.legacy.cluster.kafka.us-east-1.amazonaws.com:9096
    region: us-east-1
    auth_type: SASL_SCRAM
    scram_secret_arn: arn:aws:secretsmanager:us-east-1:123:secret:msk-legacy-creds

  cust-mtls:
    bootstrap_servers: b-1.cust.cluster.kafka.us-east-1.amazonaws.com:9094
    region: us-east-1
    auth_type: MTLS
    cert_path: /etc/msk-mcp/certs/cust.crt
    key_path:  /etc/msk-mcp/certs/cust.key
    ca_path:   /etc/msk-mcp/certs/cust-ca.crt
```

IAM is fully implemented. SCRAM and mTLS are stubbed in `kafka_clients.py` and `client_properties.py` — the registry shape and dispatch logic are in place; the Secrets Manager fetch / certificate plumbing is the work to lift them.

## Deployment to ECS Fargate

See `scripts/deploy_fargate.sh` for the manual ECR push + ECS task-def update sequence. Prerequisites:

- ECR repo exists.
- ECS cluster + Fargate service in `awsvpc` mode.
- Task IAM role grants `kafka-cluster:Connect`, `kafka-cluster:Describe*`, `kafka-cluster:ReadData` on each cluster ARN in the registry. SCRAM secrets need `secretsmanager:GetSecretValue` on those secret ARNs.
- VPC Lattice service points at the Fargate service (IP target type, port 8080). The Lattice auth policy enforces SigV4 from the DevOps Agent caller principal.
- Security group allows outbound to MSK broker ports (9092 / 9094 / 9096 / 9098).
- `/health` endpoint is wired for the Lattice target group (returns `200 {"status":"ok"}`).

CDK / CloudFormation IaC is a follow-up task once the topology is stable.

## Project layout

```
msk-devops-agent-mcp/
├── Dockerfile                                # Multi-stage: Kafka 3.9 CLI + IAM JAR + Python deps
├── pyproject.toml                            # Pinned deps, ruff/mypy/pytest config
├── config/clusters.yaml.example              # Registry template (IAM, SCRAM, mTLS shapes)
├── scripts/
│   ├── run_local.sh                          # Local dev loop
│   ├── build_image.sh                        # docker || finch detect
│   └── deploy_fargate.sh                     # Manual ECR push + ECS update
├── src/msk_mcp/
│   ├── server.py                             # FastMCP app + Starlette wrapper + /health
│   ├── config.py                             # Pydantic models for clusters.yaml + env vars
│   ├── kafka_clients.py                      # AdminClient factory (IAM live; SCRAM/mTLS stubbed)
│   ├── client_properties.py                  # Per-cluster client.properties renderer
│   ├── cli_executor.py                       # Async subprocess (timeout, semaphore, SIGTERM/SIGKILL)
│   ├── concurrency.py                        # Two-tier Bouncer
│   ├── errors.py                             # Structured error taxonomy
│   ├── logging_setup.py                      # structlog JSON + correlation-ID middleware
│   ├── http/health.py                        # /health endpoint
│   └── tools/                                # 12 tools, one file each
└── tests/
    ├── unit/                                 # Config, errors, concurrency, CLI executor, etc.
    ├── tools/                                # Per-tool tests with mocked AdminClient / subprocess
    └── integration/                          # In-process FastMCP server, both execution paths
```
