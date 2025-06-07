---
title: Partitioning
sidebar_position: 2
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Partitioning

## Partitioned Tables
In Fluss, a **Partitioned Table** organizes data based on one or more partition keys, providing a way to improve query performance and manageability for large datasets. Partitions allow the system to divide data into distinct segments, each corresponding to specific values of the partition keys.

For partitioned tables, Fluss supports three strategies of managing partitions.
   - **Manual management partitions**, user can create new partitions or drop exists partitions. Learn how to create or drop partitions please refer to [Add Partition](engine-flink/ddl.md#add-partition) and [Drop Partition](engine-flink/ddl.md#drop-partition).
   - **Auto management partitions**, the partitions will be created based on the auto partitioning rules configured at the time of table creation, and expired partitions are automatically removed, ensuring data not expanding unlimited. See [Auto Partitioning](table-design/data-distribution/partitioning.md#auto-partitioning).
   - **Dynamic create partitions**, the partitions will be created automatically based on the data being written to the table. See [Dynamic Partitioning](table-design/data-distribution/partitioning.md#dynamic-partitioning).
   
These three strategies are orthogonal and can coexist on the same table.

### Key Benefits of Partitioned Tables
- **Improved Query Performance:** By narrowing down the query scope to specific partitions, the system reads fewer data, reducing query execution time.
- **Data Organization:** Partitions help in logically organizing data, making it easier to manage and query.
- **Scalability:** Partitioning large datasets distributes the data across smaller, manageable chunks, improving scalability.

## Restrictions
- The type of the partition keys must be STRING.
- For auto partition table, the partition keys can be one or more. If the table has only one partition key, it supports automatic creation and automatic expiration of partitions. Otherwise, only automatic expiration is allowed.
- If the table is a primary key table, the partition key must be a subset of the primary key.
- Auto-partitioning rules can only be configured at the time of creating the partitioned table; modifying the auto-partitioning rules after table creation is not supported.

## Auto Partitioning
### Example
The auto-partitioning rules are configured through table options. The following example demonstrates creating a table named `site_access` that supports automatic partitioning using Flink SQL.
```sql title="Flink SQL"
CREATE TABLE site_access(
  event_day STRING,
  site_id INT,
  city_code STRING,
  user_name STRING,
  pv BIGINT,
  PRIMARY KEY(event_day, site_id) NOT ENFORCED 
) PARTITIONED BY (event_day) WITH (
  'table.auto-partition.enabled' = 'true',
  'table.auto-partition.time-unit' = 'YEAR',
  'table.auto-partition.num-precreate' = '5',
  'table.auto-partition.num-retention' = '2',
  'table.auto-partition.time-zone' = 'Asia/Shanghai'
);
```
In this case, when automatic partitioning occurs (Fluss will periodically operate on all tables in the background), four partitions are pre-created with a partition granularity of YEAR, retaining two historical partitions. The time zone is set to Asia/Shanghai.


### Table Options
| Option                             | Type    | Required | Default              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|------------------------------------|---------|----------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table.auto-partition.enabled       | Boolean | no       | false                | Whether enable auto partition for the table. Disable by default. When auto partition is enabled, the partitions of the table will be created automatically.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| table.auto-partition.key           | String  | no       | (none)               | This configuration defines the time-based partition key to be used for auto-partitioning when a table is partitioned with multiple keys. Auto-partitioning utilizes a time-based partition key to handle partitions automatically, including creating new ones and removing outdated ones, by comparing the time value of the partition with the current system time. In the case of a table using multiple partition keys (such as a composite partitioning strategy), this feature determines which key should serve as the primary time dimension for making auto-partitioning decisions. And If the table has only one partition key, this config is not necessary. Otherwise, it must be specified.      |
| table.auto-partition.time-unit     | ENUM    | no       | DAY                  | The time granularity for auto created partitions. The default value is 'DAY'. Valid values are 'HOUR', 'DAY', 'MONTH', 'QUARTER', 'YEAR'. If the value is 'HOUR', the partition format for auto created is yyyyMMddHH. If the value is 'DAY', the partition format for auto created is yyyyMMdd. If the value is 'MONTH', the partition format for auto created is yyyyMM. If the value is 'QUARTER', the partition format for auto created is yyyyQ. If the value is 'YEAR', the partition format for auto created is yyyy.                                                                                                                                                                                  |
| table.auto-partition.num-precreate | Integer | no       | 2                    | The number of partitions to pre-create for auto created partitions in each check for auto partition. For example, if the current check time is 2024-11-11 and the value is configured as 3, then partitions 20241111, 20241112, 20241113 will be pre-created. If any one partition exists, it'll skip creating the partition. The default value is 2, which means 2 partitions will be pre-created. If the 'table.auto-partition.time-unit' is 'DAY'(default), one precreated partition is for today and another one is for tomorrow. For a partition table with multiple partition keys, pre-create is unsupported and will be set to 0 automatically when creating table if it is not explicitly specified. |
| table.auto-partition.num-retention | Integer | no       | 7                    | The number of history partitions to retain for auto created partitions in each check for auto partition. For example, if the current check time is 2024-11-11, time-unit is DAY, and the value is configured as 3, then the history partitions 20241108, 20241109, 20241110 will be retained. The partitions earlier than 20241108 will be deleted. The default value is 7.                                                                                                                                                                                                                                                                                                                                   |
| table.auto-partition.time-zone     | String  | no       | the system time zone | The time zone for auto partitions, which is by default the same as the system time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |

### Partition Generation Rules
The time unit for the automatic partition table `auto-partition.time-unit` can take values of HOUR, DAY, MONTH, QUARTER, or YEAR. Automatic partitioning will use the following format to create partitions.

| Time Unit | Partition Format | Example    |
|-----------|------------------|------------|
| HOUR      | yyyyMMddHH       | 2024091922 |
| DAY       | yyyyMMdd         | 20240919   |
| MONTH     | yyyyMM           | 202409     |
| QUARTER   | yyyyQ            | 20241      |
| YEAR      | yyyy             | 2024       |
	
### Fluss Cluster Configuration
Below are the configuration items related to Fluss cluster and automatic partitioning.

| Option                        | Type     | Default    | Description                                            |
|-------------------------------|------------------|------------|------------------------------------------------|
| auto-partition.check.interval | Duration | 10 minutes    | The interval of auto partition check. The time interval for automatic partition checking is set to 10 minutes by default, meaning that it checks the table partition status every 10 minutes to see if it meets the automatic partitioning criteria. If it does not meet the criteria, partitions will be automatically created or deleted.    |

## Dynamic Partitioning

**Dynamic partitioning** is a feature that is enabled by default on client, allowing the client to automatically create partitions based on the data being written to the table. This feature is especially valuable when the set of partitions is not known in advance, eliminating the need for manual partition creation. It is also particularly useful when working with multi-field partitions, as auto-partitioning currently only supports single-field partitioning creation.

Please note that the number of dynamically created partitions is also subject to the `max.partition.num` and `max.bucket.num` limit configured on the Fluss cluster.

### Client Options
| Option                                            | Type    | Required | Default              | Description                                                                                                                                                                   |
|---------------------------------------------------|---------|----------|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| client.writer.dynamic-create-partition.enabled    | Boolean | no       | true                 | Whether to enable dynamic partition creation for the client writer. When enabled, new partitions are automatically created if they don't already exist during data writes.    |
