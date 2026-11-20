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
and logs while a bucket has no ready leader, for example during a rolling restart. Lookups for a
bucket that has no leader in the client's metadata back off the same way, per bucket and without
counting toward `client.lookup.max-retries`. As a result, these lookups may resume later after a
new leader is elected than before, unless the client's metadata gets the leader earlier.
`client.lookup.retry-backoff-max` caps only the backoff: batching (`client.lookup.batch-timeout`),
queueing and the metadata request that finds the leader can delay the lookups further.

`client.lookup.queue-size` limits only the initial input queue. Lookups waiting for a leader or for
a retry are outside this limit, so the total number of pending lookups can grow without bound.
Adding a lookup may still block while the input queue is full, but the client no longer blocks on
metadata requests for each lookup of a bucket without a leader, so that queue now fills up less
often while a leader is missing. Applications that issue asynchronous lookups should bound their
outstanding lookups, for example with the capacity of a Flink asynchronous lookup join.

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
