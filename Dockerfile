# syntax=docker/dockerfile:1.7

# Stage 1: download Kafka 3.8 CLI + aws-msk-iam-auth uber-JAR for IAM auth.
FROM eclipse-temurin:17-jre-jammy AS kafka-stage

ARG KAFKA_VERSION=3.9.2
ARG SCALA_VERSION=2.13
ARG MSK_IAM_AUTH_VERSION=2.2.0

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Use dlcdn (current versions) and fall back to archive (older versions) so
# the build works regardless of where the requested KAFKA_VERSION lives.
WORKDIR /tmp
RUN set -eux; \
    for base in \
      "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}" \
      "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}" \
    ; do \
      if curl -fsSL "${base}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -o kafka.tgz; then \
        echo "Downloaded from ${base}"; break; \
      fi; \
    done; \
    test -s kafka.tgz; \
    mkdir -p /opt/kafka; \
    tar -xzf kafka.tgz -C /opt/kafka --strip-components=1; \
    rm kafka.tgz

# IAM auth JAR (and its transitive deps bundled by AWS).
RUN curl -fsSL "https://github.com/aws/aws-msk-iam-auth/releases/download/v${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar" \
      -o /opt/kafka/libs/aws-msk-iam-auth-all.jar


# Stage 2: runtime image — Python 3.11 + JRE (from stage 1) + Kafka CLI + app.
FROM python:3.11-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy JRE from the Temurin-based stage 1 — avoids relying on Debian's apt
# repos for an OpenJDK package (the version available there shifts over time).
COPY --from=kafka-stage /opt/java /opt/java
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Kafka CLI from stage 1.
COPY --from=kafka-stage /opt/kafka /opt/kafka
ENV PATH="/opt/kafka/bin:${PATH}"

# Non-root user.
RUN useradd --create-home --shell /usr/sbin/nologin --uid 1000 mskmcp \
 && mkdir -p /etc/msk-mcp /tmp/msk-mcp \
 && chown -R mskmcp:mskmcp /tmp/msk-mcp

WORKDIR /app
COPY pyproject.toml ./
COPY src/ ./src/
# Use pip (works under cross-arch QEMU emulation; uv segfaults there).
RUN pip install --no-cache-dir .

# Bake the cluster registry into the image. The build context must contain
# config/clusters.yaml — copy clusters.yaml.example and edit it before building.
# Future iteration: pull from SSM Parameter Store at task start instead.
COPY config/clusters.yaml /etc/msk-mcp/clusters.yaml

USER mskmcp

ENV MSK_MCP_CLUSTERS_CONFIG_PATH=/etc/msk-mcp/clusters.yaml \
    MSK_MCP_KAFKA_BIN_PATH=/opt/kafka/bin \
    MSK_MCP_CLIENT_PROPERTIES_DIR=/tmp/msk-mcp \
    MSK_MCP_PORT=8080

EXPOSE 8080

CMD ["python", "-m", "msk_mcp"]
