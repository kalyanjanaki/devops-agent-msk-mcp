# MSK Debugging MCP Server

Read-only MCP server that exposes Amazon MSK debugging primitives to AWS DevOps Agent over Streamable HTTP. Runs as an ECS Fargate task behind a VPC Lattice service.

## What it does

Fills monitoring blind spots that CloudWatch metrics, Prometheus, and the MSK control plane can't cover (rebalance storms, ISR-drop attribution, stuck reassignments, etc.) by exposing Kafka AdminClient and CLI primitives as MCP tools.

**Strictly read-only.** No tool mutates Kafka state. Tools that would naturally mutate (e.g. resetting consumer offsets) instead return a proposed change as JSON plus the exact CLI command for a human to run.

## POC scope

Walking-skeleton POC ships **4 tools** to exercise both execution paths end-to-end:

| Tool | Path |
|---|---|
| `list_consumer_groups` | AdminClient |
| `describe_consumer_group` | AdminClient |
| `describe_topic` | AdminClient |
| `describe_log_dirs` | CLI subprocess (`kafka-log-dirs.sh`) |

## Local development

```sh
uv sync --extra dev
cp config/clusters.yaml.example config/clusters.yaml
# Edit clusters.yaml: point at a dev MSK cluster you can reach
./scripts/run_local.sh
```

The server listens on `http://localhost:8080/mcp`. Hit it with `curl` or any MCP client.

## Build & run container

```sh
./scripts/build_image.sh
docker run --rm -p 8080:8080 \
  -v $(pwd)/config/clusters.yaml:/etc/msk-mcp/clusters.yaml:ro \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
  msk-mcp:dev
```

## Deployment (manual, POC)

See `scripts/deploy_fargate.sh` and the [Deployment](#deployment) section below. CDK/CloudFormation is deferred until after the skeleton is validated.

## Configuration

| Env var | Default | Notes |
|---|---|---|
| `MSK_MCP_CLUSTERS_CONFIG_PATH` | `/etc/msk-mcp/clusters.yaml` | Cluster registry file |
| `MSK_MCP_PORT` | `8080` | HTTP port |
| `MSK_MCP_TOOL_CONCURRENCY` | `16` | Max concurrent tool calls |
| `MSK_MCP_CLI_CONCURRENCY` | `4` | Max concurrent CLI subprocesses (JVM fan-out cap) |
| `MSK_MCP_DEFAULT_TIMEOUT_SECONDS` | `30` | Per-tool timeout |
| `MSK_MCP_LOG_LEVEL` | `INFO` |  |

## Architecture

- **Execution**: Hybrid — `confluent-kafka-python` AdminClient for fast tools; CLI subprocess for tools where the AdminClient API is awkward (`describe_log_dirs`, future `preview_offset_reset`).
- **Auth from DevOps Agent**: VPC Lattice SigV4 at the network layer. The MCP server is plain HTTP behind Lattice.
- **Auth to MSK**: IAM, SASL/SCRAM, mTLS (per-cluster in `clusters.yaml`). POC enables IAM only; SCRAM and mTLS are stubbed.
- **MCP SDK**: FastMCP with `transport="streamable-http"`.

See `/Users/kjjanaki/.claude/plans/composed-gathering-cherny.md` for the full plan.

## Deployment

See `scripts/deploy_fargate.sh` for the manual ECR push + ECS task-def update sequence. Prerequisites:

- ECR repo exists.
- ECS cluster + Fargate service in `awsvpc` mode.
- Task IAM role: `kafka-cluster:Connect`, `kafka-cluster:Describe*`, `kafka-cluster:ReadData` on the POC cluster ARN.
- VPC Lattice service routes to the Fargate service (IP target type).
- Security group allows outbound to MSK broker ports.
