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

## Lake Storage Changes

### Duplicate Lake Format Plugins Are Rejected

In v1.1, Fluss fails to load the lake storage for `datalake.format` if more than one plugin
registers that format. Previously, one of them was picked depending on load order. The error
names each plugin class and where it was loaded from.

This affects setups with the same lake jar both in `plugins/` and on the classpath (for example
in `lib/`), or with both `fluss-lake-iceberg` and `fluss-lake-iceberg-1.12` installed. Keep exactly
one plugin per format: the plugin jar under `plugins/<format>/` on servers, and one copy on the
Flink classpath for the tiering service and Flink jobs that read lake data.
