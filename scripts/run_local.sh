#!/usr/bin/env bash
# Run the MCP server locally against a real dev MSK cluster.
# Requires: uv, AWS credentials in env or ~/.aws, network reachability to MSK.
set -euo pipefail

cd "$(dirname "$0")/.."

export MSK_MCP_CLUSTERS_CONFIG_PATH="${MSK_MCP_CLUSTERS_CONFIG_PATH:-$PWD/config/clusters.yaml}"
export MSK_MCP_PORT="${MSK_MCP_PORT:-8080}"
export MSK_MCP_LOG_LEVEL="${MSK_MCP_LOG_LEVEL:-INFO}"
export MSK_MCP_CLIENT_PROPERTIES_DIR="${MSK_MCP_CLIENT_PROPERTIES_DIR:-/tmp/msk-mcp}"

# Local Kafka CLI defaults to /opt/kafka/bin (matches the container).
# Override if you have it installed elsewhere on your laptop.
export MSK_MCP_KAFKA_BIN_PATH="${MSK_MCP_KAFKA_BIN_PATH:-/opt/kafka/bin}"

if [[ ! -f "$MSK_MCP_CLUSTERS_CONFIG_PATH" ]]; then
  echo "Cluster registry not found at $MSK_MCP_CLUSTERS_CONFIG_PATH" >&2
  echo "Copy config/clusters.yaml.example and edit it with a dev cluster's bootstrap servers." >&2
  exit 1
fi

mkdir -p "$MSK_MCP_CLIENT_PROPERTIES_DIR"

echo "Starting msk-mcp on port $MSK_MCP_PORT (cluster registry: $MSK_MCP_CLUSTERS_CONFIG_PATH)"
exec uv run python -m msk_mcp
