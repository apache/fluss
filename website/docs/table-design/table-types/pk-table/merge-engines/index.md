---
sidebar_position: 1
---

# Merge Engines

The **Merge Engine** in Fluss is a core component designed to efficiently handle and consolidate data updates for PrimaryKey Tables.
It offers users the flexibility to define how incoming data records are merged with existing records sharing the same primary key.
The default merge engine in Fluss retains the latest record for a given primary key.
However, users can specify a different merge engine to customize the merging behavior according to their specific use cases

The following merge engines are supported:

1. [FirstRow Merge Engine](/docs/table-design/table-types/pk-table/merge-engines/first-row)
2. [Versioned Merge Engine](/docs/table-design/table-types/pk-table/merge-engines/versioned)