---
title: "Client Support Matrix"
sidebar_position: 2
---

# Client Feature Support Matrix

Fluss has a rich set of features and native data types available to users. The following tables summarize the features available across various Fluss clients. 

## Data Operations

These data operations are available under TableAppend, TableScan, TableUpsert and TableLookup interfaces.

| Table Type   | Operations                 | [Java Client](/apis/java-client.md) | Rust Client | Python Client | C++ Client |
|--------------|----------------------------|-------------------------------------|-------------|---------------|------------|
| Log          | Append                     | ✔️                                  | ✔️          | ✔️            | ✔️         |
| Log          | Typed Append               | ✔️                                  |             |               |            |
| Log          | Scan                       | ✔️                                  | ✔️          | ✔️            | ✔️         |
| Log          | Scan with Projection       | ✔️                                  | ✔️          | ✔️            | ✔️         |
| Log          | Typed Scan                 | ✔️                                  |             |               |            |
| Log          | Batch Scan with Limit      | ✔️                                  |             |               |            |
| Primary Key  | Upsert                     | ✔️                                  | ✔️          |               |            |
| Primary Key  | Upsert with Partial Update | ✔️                                  | ✔️          |               |            |
| Primary Key  | Typed Upsert               | ✔️                                  |             |               |            |
| Primary Key  | Delete                     | ✔️                                  | ✔️          |               |            |
| Primary Key  | Lookup                     | ✔️                                  | ✔️          |               |            |
| Primary Key  | Prefix Lookup              | ✔️                                  |             |               |            |
| Primary Key  | Typed Lookup               | ✔️                                  |             |               |            |
| Primary Key  | Batch Scan with Limit      | ✔️                                  |             |               |            |
| Primary Key  | Batch Scan (Snapshot)      | ✔️                                  |             |               |            |

:::tip
For more details, see [Table Overview](/table-design/overview.md).
:::

## Data Types

Client support for Fluss data types are as follows:

| DataType                                                        | [Java Client](/apis/java-client.md) | Rust Client | Python Client | C++ Client |
|-----------------------------------------------------------------|-------------------------------------|-------------|---------------|------------|
| BOOLEAN                                                         | ✔️                                  | ✔️          | ✔️            | ✔️         |
| TINYINT                                                         | ✔️                                  | ✔️          | ✔️            | ✔️         |
| SMALLINT                                                        | ✔️                                  | ✔️          | ✔️            | ✔️         |
| INT                                                             | ✔️                                  | ✔️          | ✔️            | ✔️         |
| BIGINT                                                          | ✔️                                  | ✔️          | ✔️            | ✔️         |
| FLOAT                                                           | ✔️                                  | ✔️          | ✔️            | ✔️         |
| DOUBLE                                                          | ✔️                                  | ✔️          | ✔️            | ✔️         |
| CHAR(n)                                                         | ✔️                                  | ✔️          | ✔️            | ✔️         |
| STRING                                                          | ✔️                                  | ✔️          | ✔️            | ✔️         |
| DECIMAL(p, s)                                                   | ✔️                                  | ✔️          | ✔️            |            |
| DATE                                                            | ✔️                                  | ✔️          | ✔️            | ✔️         |
| TIME                                                            | ✔️                                  | ✔️          | ✔️            | ✔️         |
| TIME(p)                                                         | ✔️                                  | ✔️          | ✔️            | ✔️         |
| TIMESTAMP                                                       | ✔️                                  | ✔️          | ✔️            | ✔️         |
| TIMESTAMP(p)                                                    | ✔️                                  | ✔️          | ✔️            | ✔️         |
| TIMESTAMP_LTZ                                                   | ✔️                                  | ✔️          | ✔️            | ✔️         |
| TIMESTAMP_LTZ(p)                                                | ✔️                                  | ✔️          | ✔️            | ✔️         |
| BINARY(n)                                                       | ✔️                                  | ✔️          | ✔️            | ✔️         |
| BYTES                                                           | ✔️                                  | ✔️          | ✔️            | ✔️         |
| ARRAY\<t\>                                                      | ✔️                                  |             |               |            |
| MAP\<kt, vt\>                                                   | ✔️                                  |             |               |            |
| ROW\<n0 t0, n1 t1, ...\><br/>ROW\<n0 t0 'd0', n1 t1 'd1', ...\> | ✔️                                  |             |               |            |

:::tip
For more details, see [Data Types](table-design/data-types.md).
:::

## Admin Operations

Admin operations are available under FlussAdmin interface.

| Entity    | Operations             | [Java Client](/apis/java-client.md) | Rust Client | Python Client | C++ Client |
|-----------|------------------------|-------------------------------------|-------------|---------------|------------|
| Database  | CreateDatabase         | ✔️                                  | ✔️          |               |            |
| Database  | DropDatabase           | ✔️                                  | ✔️          |               |            |
| Database  | DatabaseExists         | ✔️                                  | ✔️          |               |            |
| Database  | GetDatabaseInfo        | ✔️                                  | ✔️          |               |            |
| Database  | ListDatabases          | ✔️                                  | ✔️          |               |            |
| Table     | AlterTable             | ✔️                                  |             |               |            |
| Table     | CreateTable            | ✔️                                  | ✔️          | ✔️            | ✔️         |
| Table     | DropTable              | ✔️                                  | ✔️          |               | ✔️         |
| Table     | GetTableSchema         | ✔️                                  |             |               |            |
| Table     | GetTableInfo           | ✔️                                  | ✔️          | ✔️            | ✔️         |
| Table     | ListTables             | ✔️                                  | ✔️          |               |            |
| Partition | CreatePartition        | ✔️                                  |             |               |            |
| Partition | DropPartition          | ✔️                                  |             |               |            |
| Partition | ListPartitionInfos     | ✔️                                  |             |               |            |
| Snapshot  | GetKvSnapshotMetadata  | ✔️                                  |             |               |            |
| Snapshot  | GetLatestKvSnapshots   | ✔️                                  |             |               |            |
| Snapshot  | GetLatestLakeSnapshot  | ✔️                                  | ✔️          | ✔️            | ✔️         |
| Bucket    | ListOffsets            | ✔️                                  | ✔️          |               | ✔️         |
| Cluster   | AlterClusterConfigs    | ✔️                                  |             |               |            |
| Cluster   | DescribeClusterConfigs | ✔️                                  |             |               |            |
| Cluster   | CancelRebalance        | ✔️                                  |             |               |            |
| Cluster   | Rebalance              | ✔️                                  |             |               |            |
| Cluster   | ListRebalanceProgress  | ✔️                                  |             |               |            |
| Server    | AddServerTag           | ✔️                                  |             |               |            |
| Server    | RemoveServerTag        | ✔️                                  |             |               |            |
| ACL       | CreateAcls             | ✔️                                  |             |               |            |
| ACL       | DropAcls               | ✔️                                  |             |               |            |
| ACL       | ListAcls               | ✔️                                  |             |               |            |

## Data Lake Formats

| Format  | [Java Client](/apis/java-client.md) | Rust Client | Python Client | C++ Client |
|---------|-------------------------------------|-------------|---------------|------------|
| Iceberg | ✔️                                  |             |               |            |
| Lance   | ✔️                                  | ✔️          |               |            |
| Paimon  | ✔️                                  |             |               |            |

:::tip
For more details, see [Streaming Lakehouse](/streaming-lakehouse/overview.md).
:::