---
title: "Merge Engines"
sidebar_position: 1
---

# Merge Engines

The **Merge Engine** in Fluss is a core component designed to efficiently handle and consolidate data updates for Primary Key Tables.
It offers users the flexibility to define how incoming data records are merged with existing records sharing the same primary key.
However, users can specify a different merge engine to customize the merging behavior according to their specific use cases.

The following merge engines are supported:

1. [Default Merge Engine (LastRow)](table-design/table-types/pk-table/merge-engines/default.md)
2. [FirstRow Merge Engine](table-design/table-types/pk-table/merge-engines/first-row.md)
3. [Versioned Merge Engine](table-design/table-types/pk-table/merge-engines/versioned.md)
4. [Aggregation Merge Engine](table-design/table-types/pk-table/merge-engines/aggregation.md)
