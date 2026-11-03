---
title: Upgrade Notes
sidebar_position: 4
---

# Upgrade Notes from v1.0 to v1.1

## Server Configuration Changes

### Active Segment Retention Rolling Enabled by Default

In v1.1, `log.retention.roll-active-segment.enabled` defaults to `true` instead of `false`.
Eligible expired active segments can now roll without new writes, allowing upload and cleanup
on low-traffic tables.

After upgrading, previously retained expired logs may be cleaned up. Review `table.log.ttl`
and `table.log.local-ttl` to ensure they cover consumer lag and recovery needs. KV row retention
is unchanged.

Explicit settings remain effective. To retain the previous behavior, set
`log.retention.roll-active-segment.enabled: false` before upgrading.

## Helm Chart Changes

### Readiness Probes Need a Credential When Client SASL Is Enabled

In v1.1, the chart fails to render when `security.client.sasl.mechanism` is set and either
readiness probe has no credential. Previously, the chart rendered without one, and on a cluster
with client SASL the TabletServer probe failed to authenticate until its 20-minute health check
budget ran out. The coordinator readiness probe is new in this release and also authenticates
against the client listener.

Before upgrading the chart, set `coordinator.readinessProbe.healthCheckAuth` and
`tablet.readinessProbe.healthCheckAuth`, or add a `READINESS_HEALTH_CHECK_AUTH` entry to
`secrets.env` to read the credential from a Secret. See
[Coordinator Readiness Probe](../../install-deploy/deploying-with-helm.md#coordinator-readiness-probe).

## Client Configuration Changes

### Lookup Retries Back Off by Default

In v1.1, a lookup or prefix lookup that fails with a retriable error, such as
`NOT_LEADER_OR_FOLLOWER`, is retried after an exponential backoff instead of immediately. The
delay starts at `client.lookup.retry-backoff` (default `100ms`) and doubles up to
`client.lookup.retry-backoff-max` (default `1s`). This stops clients from flooding TabletServers
and logs while a bucket has no ready leader, for example during a rolling restart.

The first retry after a leader change now waits about 100ms. To retain the previous behavior,
set `client.lookup.retry-backoff: 0`.

## Lake Storage Changes

### Duplicate Lake Format Plugins Are Rejected

In v1.1, Fluss fails to load the lake storage for `datalake.format` if more than one plugin
registers that format. Previously, one of them was picked depending on load order. The error
names each plugin class and where it was loaded from.

This affects setups with the same lake jar both in `plugins/` and on the classpath (for example
in `lib/`), or with both `fluss-lake-iceberg` and `fluss-lake-iceberg-1.12` installed. Keep exactly
one plugin per format: the plugin jar under `plugins/<format>/` on servers, and one copy on the
Flink classpath for the tiering service and Flink jobs that read lake data.
