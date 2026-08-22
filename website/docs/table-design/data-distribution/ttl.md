---
title: TTL
sidebar_position: 3
---

# TTL

Fluss supports TTL for data by setting the TTL attribute for tables with `'table.log.ttl' = '<duration>'` (default is 7 days). Fluss can periodically and automatically check for and clean up expired data in the table.

For log tables, this attribute indicates the expiration time of the log table data.
For primary key tables, this attribute indicates the expiration time of the changelog and does not represent the expiration time of the primary key table data. If you also want the data in the primary key table to expire automatically, please use [auto partitioning](partitioning.md#auto-partitioning).

When tiered storage is enabled, `table.log.local-ttl` can be used to control how long copied local log segments are retained. If it is not configured, it falls back to `table.log.ttl` for backward compatibility. A non-positive local TTL disables TTL-based local cleanup. When both TTLs are positive, the local TTL must be less than or equal to `table.log.ttl`.
