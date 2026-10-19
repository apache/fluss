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
