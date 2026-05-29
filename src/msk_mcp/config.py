from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class AuthType(str, Enum):
    IAM = "IAM"
    SASL_SCRAM = "SASL_SCRAM"
    MTLS = "MTLS"


class IamCluster(BaseModel):
    auth_type: Literal[AuthType.IAM]
    bootstrap_servers: str
    region: str
    description: str | None = None


class ScramCluster(BaseModel):
    auth_type: Literal[AuthType.SASL_SCRAM]
    bootstrap_servers: str
    region: str
    scram_secret_arn: str
    description: str | None = None


class MtlsCluster(BaseModel):
    auth_type: Literal[AuthType.MTLS]
    bootstrap_servers: str
    region: str
    cert_path: str
    key_path: str
    ca_path: str
    description: str | None = None


ClusterConfig = Annotated[
    IamCluster | ScramCluster | MtlsCluster,
    Field(discriminator="auth_type"),
]


class ClustersRegistry(BaseModel):
    clusters: dict[str, ClusterConfig]

    @field_validator("clusters")
    @classmethod
    def _at_least_one_cluster(cls, v: dict[str, ClusterConfig]) -> dict[str, ClusterConfig]:
        if not v:
            raise ValueError("clusters.yaml must define at least one cluster")
        return v

    def get(self, cluster_id: str) -> ClusterConfig:
        try:
            return self.clusters[cluster_id]
        except KeyError as e:
            valid = sorted(self.clusters.keys())
            raise UnknownClusterError(cluster_id, valid) from e

    def ids(self) -> list[str]:
        return sorted(self.clusters.keys())


class UnknownClusterError(Exception):
    def __init__(self, cluster_id: str, valid_ids: list[str]):
        self.cluster_id = cluster_id
        self.valid_ids = valid_ids
        super().__init__(
            f"Unknown cluster_id '{cluster_id}'. Valid: {', '.join(valid_ids)}"
        )


class Settings(BaseModel):
    clusters_config_path: Path = Path("/etc/msk-mcp/clusters.yaml")
    port: int = 8080
    tool_concurrency: int = 16
    cli_concurrency: int = 4
    default_timeout_seconds: float = 30.0
    log_level: str = "INFO"
    kafka_bin_path: Path = Path("/opt/kafka/bin")
    client_properties_dir: Path = Path("/tmp/msk-mcp")
    # Extra CLASSPATH for the Kafka CLI subprocess (e.g. aws-msk-iam-auth uber-JAR
    # when it's not already in /opt/kafka/libs/). Empty in the container image.
    kafka_classpath: str | None = None

    @model_validator(mode="after")
    def _validate_positives(self) -> Settings:
        if self.tool_concurrency < 1:
            raise ValueError("MSK_MCP_TOOL_CONCURRENCY must be >= 1")
        if self.cli_concurrency < 1:
            raise ValueError("MSK_MCP_CLI_CONCURRENCY must be >= 1")
        if self.default_timeout_seconds <= 0:
            raise ValueError("MSK_MCP_DEFAULT_TIMEOUT_SECONDS must be > 0")
        return self


def load_settings(env: dict[str, str] | None = None) -> Settings:
    e = env if env is not None else os.environ
    return Settings(
        clusters_config_path=_expand_path(
            e.get("MSK_MCP_CLUSTERS_CONFIG_PATH", "/etc/msk-mcp/clusters.yaml")
        ),
        port=int(e.get("MSK_MCP_PORT", "8080")),
        tool_concurrency=int(e.get("MSK_MCP_TOOL_CONCURRENCY", "16")),
        cli_concurrency=int(e.get("MSK_MCP_CLI_CONCURRENCY", "4")),
        default_timeout_seconds=float(e.get("MSK_MCP_DEFAULT_TIMEOUT_SECONDS", "30")),
        log_level=e.get("MSK_MCP_LOG_LEVEL", "INFO"),
        kafka_bin_path=_expand_path(e.get("MSK_MCP_KAFKA_BIN_PATH", "/opt/kafka/bin")),
        client_properties_dir=_expand_path(
            e.get("MSK_MCP_CLIENT_PROPERTIES_DIR", "/tmp/msk-mcp")
        ),
        kafka_classpath=e.get("MSK_MCP_KAFKA_CLASSPATH") or None,
    )


def _expand_path(s: str) -> Path:
    return Path(s).expanduser()


def load_registry(path: Path) -> ClustersRegistry:
    if not path.exists():
        raise FileNotFoundError(f"Cluster registry not found at {path}")
    with path.open("r") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must be a YAML mapping at the top level")
    return ClustersRegistry.model_validate(raw)
