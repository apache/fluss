---
title: TTL
sidebar_position: 3
---

# TTL

Fluss supports TTL for data by setting the TTL attribute for tables with `'table.log.ttl' = '<duration>'` (default is 7 days). Fluss can periodically and automatically check for and clean up expired data in the table.

For log tables, this attribute indicates the expiration time of the log table data.
For primary key tables, this attribute indicates the expiration time of the changelog and does not represent the expiration time of the primary key table data. If you also want the data in the primary key table to expire automatically, please use [auto partitioning](partitioning.md#auto-partitioning).

When tiered storage is enabled, `table.log.local-ttl` can be used to retain copied local log segments for a shorter period than remote log segments. It defaults to `table.log.ttl`. A configured local TTL must be greater than zero and, when `table.log.ttl` is positive, less than or equal to `table.log.ttl`.
