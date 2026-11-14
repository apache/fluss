---
sidebar_position: 7
title: "Deploying Fluss Gateway"
---

# Deploying Fluss Gateway

:::caution Preview

Fluss Gateway is introduced as a preview in Fluss 1.0. Its API and
configuration may change in later releases.

:::

Fluss Gateway is a stateless REST service distributed as a Linux binary and a
multi-architecture container image.

For the REST API itself, see [Fluss Gateway](../gateway/index.md).

## Requirements

**Platforms**

The convenience binary and the container image target Linux on `amd64`
(x86-64) and `arm64` (aarch64). The binary requires a glibc 2.36 baseline or
newer (Debian Bookworm or equivalent). The container image is based on
`debian:bookworm-slim`.

**Fluss cluster**

Every Gateway instance needs network access to the
`gateway.cluster.<id>.bootstrap.servers` of each configured Fluss cluster and
to the CoordinatorServer and TabletServer addresses the clients receive from
metadata.

**Ports**

| Port | Protocol | Purpose |
| --- | --- | --- |
| 8080 | HTTP | REST API (default bind `127.0.0.1` in the binary distribution, `0.0.0.0` in the container image) |
| 9095 | HTTP | Prometheus metrics endpoint |

## Get the Gateway

Starting with Fluss 1.0, Gateway release artifacts are published alongside the
Fluss release:

| Artifact | Location | Identifier |
| --- | --- | --- |
| Binary distribution | [Apache downloads](https://fluss.apache.org/downloads) | `fluss-gateway-<version>-bin-linux-amd64.tgz` / `...-arm64.tgz` |
| Container image | [Docker Hub](https://hub.docker.com/r/apache/fluss-gateway) | `apache/fluss-gateway:<version>` (multi-arch `amd64`/`arm64`) |

Replace `<version>` in the examples below with the Fluss release version.
Verify the archive checksum (`.sha512`) and signature (`.asc`) published
alongside the release before extracting it.

For an unreleased development version, build the distribution or image from
source as described in the
[fluss-gateway README](https://github.com/apache/fluss/blob/main/fluss-gateway/README.md):
`tools/releasing/create_gateway_release.sh` produces the binary archive and
`just image` (or `docker/fluss-gateway/build.sh`) builds the container image.

### Binary distribution layout

```text
fluss-gateway-<version>-bin-linux-<arch>/
├── bin/
│   ├── fluss-gateway        # the gateway executable
│   └── fluss-gateway.sh     # wrapper: resolves FLUSS_HOME and default config path
├── conf/
│   └── gateway.yaml         # default configuration
├── openapi.yaml             # OpenAPI 3.1 specification
├── DEPENDENCIES.rust.tsv
├── LICENSE
└── NOTICE
```

## Configure the Gateway

Configuration is a flat `gateway.yaml` with dot-separated keys. Environment
variables override file values (`FLUSS_GATEWAY__*`), which is the preferred way
to inject settings and secrets into containerized deployments; see
[Configuration](../gateway/index.md#configuration) for the mapping rules.

The most relevant deployment options:

| Key | Default | Purpose |
| --- | --- | --- |
| `gateway.rest.listen` | `127.0.0.1:8080` | REST bind address. The container image sets `0.0.0.0:8080` through `FLUSS_GATEWAY__REST__LISTEN` |
| `gateway.rest.request-timeout` | `30s` | Server-side deadline for one REST request |
| `gateway.rest.write.max-request-bytes` | `32MiB` | Maximum request body size |
| `gateway.rest.write.max-rows` | `10000` | Maximum rows per write batch |
| `gateway.rest.write.max-concurrent-requests` | `64` | Write admission limit per instance |
| `gateway.rest.metadata.max-concurrent-requests` | `16` | Metadata admission limit per instance |
| `gateway.rest.write.rate-limit.*` | disabled | Optional per-instance write rate limiting |
| `gateway.clusters` | `default` | Comma-separated logical cluster IDs |
| `gateway.cluster.<id>.bootstrap.servers` | `127.0.0.1:9123` | Fluss client bootstrap servers |
| `gateway.cluster.<id>.connect-timeout` | `10s` | Timeout for establishing the native Fluss client connection |
| `gateway.cluster.<id>.connection.idle-timeout` | `10m` | How long an unused shared connection remains cached |
| `gateway.cluster.<id>.connection.security.protocol` | `plaintext` | `plaintext` or `sasl` |
| `gateway.cluster.<id>.connection.service.account` / `...service.secret` | — | SASL/PLAIN service credentials |
| `gateway.metrics.exporter.prometheus.listen` | `127.0.0.1:9095` | Prometheus bind address. The container image sets `0.0.0.0:9095` |
| `gateway.shutdown.drain-timeout` | `30s` | Graceful-shutdown drain budget for in-flight requests |

See
[`conf/gateway.yaml`](https://github.com/apache/fluss/blob/main/fluss-gateway/conf/gateway.yaml)
for all settings and defaults.

:::note Secrets

Do not bake service credentials into an image or commit them to a packaged
`gateway.yaml`. Mount a separate file or inject the corresponding
`FLUSS_GATEWAY__*` variables from the runtime's secret store.

:::

## Run the binary distribution

Extract the archive and edit `conf/gateway.yaml` to point at your Fluss
cluster, then start the foreground process:

```bash
GATEWAY_VERSION=1.0.0
tar -xzf "fluss-gateway-${GATEWAY_VERSION}-bin-linux-amd64.tgz"
cd "fluss-gateway-${GATEWAY_VERSION}-bin-linux-amd64"

# point the default cluster at your Fluss bootstrap servers, then:
bin/fluss-gateway.sh
```

The wrapper resolves `FLUSS_HOME` from its own location and reads
`conf/gateway.yaml`. It forwards additional CLI options to the binary:

| Option | Effect |
| --- | --- |
| `--config FILE` | Use an alternative configuration file |
| `--bind-address ADDR` | Override `gateway.rest.listen` |
| `--version` | Print the binary version and exit |

The Gateway runs in the foreground and does not daemonize itself. Use a process
supervisor such as systemd when running the binary in production. Configuration
errors exit with status `2`; bind or serving failures exit with status `1`.

## Run the container image

The image runs as the non-root `fluss` user (UID/GID 9999), ships a
`HEALTHCHECK` against `/health`, and binds the REST and Prometheus listeners to
`0.0.0.0` through typed environment defaults.

### Run with Docker

```bash
GATEWAY_VERSION=1.0.0
docker run --rm \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --stop-timeout 35 \
  -p 127.0.0.1:8080:8080 \
  -p 127.0.0.1:9095:9095 \
  -e FLUSS_GATEWAY__CLUSTER__DEFAULT__BOOTSTRAP__SERVERS=host.docker.internal:9123 \
  "apache/fluss-gateway:${GATEWAY_VERSION}"
```

The Gateway needs neither a writable root filesystem nor Linux capabilities:
these options make the root filesystem read-only, drop all capabilities, and
prevent the process from gaining additional privileges.

`--stop-timeout 35` keeps the stop timeout above the drain budget (see
[Graceful shutdown](#graceful-shutdown-and-upgrades)). On Linux hosts without
`host.docker.internal`, use the cluster DNS name or container-network alias. To
mount a configuration file instead of using environment variables, add
`-v /path/to/gateway.yaml:/opt/fluss/conf/gateway.yaml:ro`.

### Test with Docker Compose

For local testing, start a Fluss cluster with the Compose file in
[Deploying with Docker](./deploying-with-docker.md). Because the Gateway runs
inside the Compose network, configure the CoordinatorServer and TabletServer
`advertised.listeners` with their Compose service names instead of `localhost`,
then add this service to the same file:

```yaml
services:
  gateway:
    image: apache/fluss-gateway:<version>
    restart: always
    depends_on: [tablet-server]
    ports:
      - "8080:8080"
    environment:
      FLUSS_GATEWAY__CLUSTER__DEFAULT__BOOTSTRAP__SERVERS: coordinator-server:9123
    read_only: true
    cap_drop: [ALL]
    security_opt:
      - no-new-privileges:true
    stop_grace_period: 35s
```

Then verify the Gateway:

```bash
docker compose up -d
curl --fail http://127.0.0.1:8080/health
curl --fail http://127.0.0.1:8080/ready
```

## Health checks and graceful shutdown

Map `GET /health` to liveness checks and `GET /ready` to readiness checks
(load balancer or container orchestrator). The endpoint semantics are described
in [Health checks](../gateway/index.md#health-checks); in particular, `/ready`
stays up during a Fluss outage, so backend problems do not mark an instance
unready.

### Graceful shutdown and upgrades

On `SIGTERM` (or Ctrl-C), the Gateway:

1. flips `/ready` to failing so load balancers and orchestrators stop sending new
   requests;
2. stops accepting new requests and drains in-flight requests within
   `gateway.shutdown.drain-timeout` (default `30s`);
3. closes the shared Fluss service connections and exits with status `0`.

Container and process stop timeouts must be **greater** than the drain budget,
or the supervisor sends `SIGKILL` mid-drain. The Docker examples use `35s`
against the `30s` default. Because instances are stateless, a rolling upgrade
only needs the readiness probe (or load-balancer removal), `SIGTERM`, and a new
instance; no socket handoff or hot-restart mechanism is provided.

## Scaling and load balancing

The Gateway holds no session, cursor, or replay state; the only per-instance
state is one lazily opened shared service connection per configured Fluss
cluster. Therefore:

- Any L4 or L7 load balancer works; no session affinity is required.
- Scale by adding instances; admission limits such as
  `gateway.rest.write.max-concurrent-requests` and optional rate limits apply
  per instance, so aggregate capacity grows linearly with the replica count.
- Requests for any configured logical cluster can be served by any instance.

## Observability

**Metrics.** When `gateway.metrics.enabled` is `true` (the default), the
Gateway exposes a Prometheus endpoint on
`gateway.metrics.exporter.prometheus.listen` (default `127.0.0.1:9095`,
`0.0.0.0:9095` in the container image). Example scrape configuration:

```yaml
scrape_configs:
  - job_name: fluss-gateway
    static_configs:
      - targets:
          - "gateway-1.example.internal:9095"
          - "gateway-2.example.internal:9095"
```

**Logs.** The Gateway logs to standard error, which the container runtime or
process supervisor captures directly. Set `RUST_LOG=debug` temporarily for
per-request access logs and connection diagnostics; the default level is
suitable for production.

## Security checklist

The 1.0 preview implements only `trust` mode (see
[Security](../gateway/index.md#security)). Before exposing a Gateway beyond a trusted
network boundary:

- Terminate TLS at an authenticated ingress or load balancer.
- Restrict access to the REST port (8080) and the Prometheus port (9095) with
  network policies or firewall rules.
- With SASL/PLAIN cluster connections, grant the shared service account only
  the required permissions.
- Run containers with the hardened flags shown above (`--read-only`,
  `--cap-drop ALL`, `no-new-privileges`).
