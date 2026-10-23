---
title: "Merge Engines"
sidebar_position: 1
---

# Merge Engines

The **Merge Engine** in Fluss is a core component designed to efficiently handle and consolidate data updates for Primary Key Tables.
It offers users the flexibility to define how incoming data records are merged with existing records sharing the same primary key.
However, users can specify a different merge engine to customize the merging behavior according to their specific use cases.

The following merge engines are supported:

1. [Default Merge Engine (LastRow)](default.md)
2. [FirstRow Merge Engine](first-row.md)
3. [Versioned Merge Engine](versioned.md)
4. [Aggregation Merge Engine](aggregation.md)

## Merge Engine Options

Options under `table.merge-engine.<engine>.*` belong to the named merge engine. They can only be configured for primary key tables using that engine. Primary key tables without an explicit `table.merge-engine` use the default engine; log tables have no merge engine.

An option belonging to another engine is rejected, even if its value is `false`. For example, `table.merge-engine.versioned.ver-column` requires the versioned engine. Engine-specific options do not override or combine with the rules of other engines. For aggregation tables, the field aggregate functions determine NULL handling.
