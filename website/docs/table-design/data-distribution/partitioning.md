---
sidebar_position: 2
---

# Partitioning

## Partitioned Tables
In Fluss, a **Partitioned Table** organizes data based on one or more partition keys, providing a way to improve query performance and manageability for large datasets. Partitions allow the system to divide data into distinct segments, each corresponding to specific values of the partition keys.

For partitioned tables, Fluss not only supports manage partitions by users, like create/drop partitions, but also supports automatic manage partitions.
   - For manually managing partitions, user can create new partitions or drop exists partitions. Learn how to create or drop partitions please refer to [Add Partition](/docs/engine-flink/ddl.md#add-partition) and [Drop Partition](/docs/engine-flink/ddl.md#drop-partition).
   - For automatically managing partitions, the partitions will be created based on the auto partitioning rules configured at the time of table creation, and expired partitions are automatically removed, ensuring data not expanding unlimited. See [Auto Partitioning Options](/docs/table-design/data-distribution/partitioning.md#auto-partitioning-options).
   - Manual management and automated management are orthogonal and can coexist on the same table

### Key Benefits of Partitioned Tables
- **Improved Query Performance:** By narrowing down the query scope to specific partitions, the system reads fewer data, reducing query execution time.
- **Data Organization:** Partitions help in logically organizing data, making it easier to manage and query.
- **Scalability:** Partitioning large datasets distributes the data across smaller, manageable chunks, improving scalability.

## Restrictions
- Only one partition key is supported, and the type of the partition key must be STRING.
- If the table is a primary key table, the partition key must be a subset of the primary key.
- Auto-partitioning rules can only be configured at the time of creating the partitioned table; modifying the auto-partitioning rules after table creation is not supported.

## Auto Partitioning Options
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


### Options
| Option                              | Type    | Required | Default              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|-------------------------------------|---------|----------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table.auto-partition.enabled        | Boolean | no       | false                | Whether enable auto partition for the table. Disable by default. When auto partition is enabled, the partitions of the table will be created automatically.                                                                                                                                                                                                                                                                                                                                                                           |
| table.auto-partition.time-unit      | ENUM    | no       | DAY                  | The time granularity for auto created partitions. The default value is 'DAY'. Valid values are 'HOUR', 'DAY', 'MONTH', 'QUARTER', 'YEAR'. If the value is 'HOUR', the partition format for auto created is yyyyMMddHH. If the value is 'DAY', the partition format for auto created is yyyyMMdd. If the value is 'MONTH', the partition format for auto created is yyyyMM. If the value is 'QUARTER', the partition format for auto created is yyyyQ. If the value is 'YEAR', the partition format for auto created is yyyy.          |
| table.auto-partition.num-precreate  | Integer | no       | 2                    | The number of partitions to pre-create for auto created partitions in each check for auto partition. For example, if the current check time is 2024-11-11 and the value is configured as 3, then partitions 20241111, 20241112, 20241113 will be pre-created. If any one partition exists, it'll skip creating the partition. The default value is 2, which means 2 partitions will be pre-created. If the 'table.auto-partition.time-unit' is 'DAY'(default), one precreated partition is for today and another one is for tomorrow. |
| table.auto-partition.num-retention  | Integer | no       | 7                    | The number of history partitions to retain for auto created partitions in each check for auto partition. For example, if the current check time is 2024-11-11, time-unit is DAY, and the value is configured as 3, then the history partitions 20241108, 20241109, 20241110 will be retained. The partitions earlier than 20241108 will be deleted. The default value is 7.                                                                                                                                                           |
| table.auto-partition.time-zone      | String  | no       | the system time zone | The time zone for auto partitions, which is by default the same as the system time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                              |

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





		