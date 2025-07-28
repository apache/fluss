---
slug: taotian-practice
title: "Fluss Joins the Apache Incubator"
sidebar_label: "The Implementation Practice Of Fluss On Taotian AB Test Analysis Platform : A Message Queue More Suitable for Real-Time OLAP"
authors: [Zhang Xinyu, Wang Lilei]
---


**Introduction:** The Data Development Team of Taotian Group has built a new generation of real-time data warehouse based on Fluss, which solves the problems of redundant data consumption, difficult data profiling, and challenges in large State operation and maintenance. Fluss integrates columnar storage and real-time update capabilities, supports column pruning, KV point query, Delta Join, and lake-stream integration, significantly reducing IO and computing resource consumption, and improving job stability and data profiling efficiency. It has been implemented on the Taotian AB Test Platform, covering core businesses such as search and recommendation, and has been verified during the 618 Grand Promotion, achieving tens of millions of traffic, second latency, a 30% reduction in resource consumption, and a State reduction of over 100TB. In the future, it will continue to deepen lake-house architecture and expand AI scenario applications.

1. # Business Background


// TODO Image Placeholder


Taotian AB Test Analysis Platform (hereinafter collectively referred to as the Babel Tower) mainly focuses on AB data of Taotian's C-end algorithms, aiming to promote scientific decision-making activities through the construction of generalized AB data capabilities. Since its inception in **2015** , it has continuously and effectively supported the analysis of Taotian's algorithm AB data for **10 years** . Currently, it is applied to **over 100** A/B testing scenarios across various business areas, including **search, recommendation, content,** **user growth****, and marketing**.


// TODO Image Placeholder


The Babel Tower provides the following capabilities:

- **AB Data Public Data Warehouse:** Serves various data applications of downstream algorithms, including: online traffic splitting, distribution alignment, general features, scenario labels, and other application scenarios.

- **Scientific Experiment Evaluation:** Implement mature scientific evaluation solutions in the industry, conduct long-term tracking of the effects of AB tests, and help businesses obtain real and reliable experimental evaluation results.

- **Multidimensional Ad Hoc OLAP Self-Service Analysis:** Through mature data solutions, it supports multidimensional and ad hoc OLAP queries, serving data effectiveness analysis across all clients, businesses, and scenarios.


# 2. Business Pain Points

Currently, the real-time data warehouse of the Babel Tower is based on technology stacks such as Flink, message queue, OLAP engine, etc., where the message queue is TT (Kafka-like architecture MQ) within the Taotian group, and the OLAP engine is Alibaba Cloud Hologres.


// TODO Image Placeholder


After consuming the message queue data collected from access logs, we perform business logic processing in Flink. However, when SQL is complex, especially when there are OrderBy and Join operations, it will cause the retraction stream processed by Flink to double, resulting in a very large Flink state and consuming a large amount of computing resources. This scenario may pose significant challenges in terms of job development and maintenance. The development cycle of jobs is also much longer compared to offline solutions. Currently, the message queue still has some limitations, and the main problems encountered are as follows:

## 2.1 Data Consumption Redundancy

In a data warehouse, write-once, read-many is a common operation mode, and each consumer typically only consumes a portion of the data. For example, in an exposure job of the Babel Tower, the consumed message queue provides 44 fields, but we only need 13 of them. Since the message queue is row-based storage, when consuming, we still need to read the data of all columns. This means that 70% of the IO for consumption has to bear 100% of the cost. This situation leads to a significant waste of network resources.

In Flink, we attempted to explicitly specify the consumption column schema in the Source and introduce a column pruning UDF operator to reduce the consumption cost of the data source, but in practice, the results were minimal and the data pipeline complexity was relatively high.

// TODO Image Placeholder

## 2.2 Data Profiling Difficulties

### 2.2.1 MQ does not support Data Point Query

In data warehouse construction, data profiling is a basic business requirement, used for problem location, case troubleshooting, etc. Two data profiling methods for message queues have been explored in production practice, both of which have advantages and disadvantages and cannot comprehensively meet business requirements.

// TODO Image Placeholder

// TODO Image Placeholder


### 2.2.2 Flink State data is not visible

In e-commerce scenarios, counting the user's first and last channels on the same day is a key indicator for measuring user acquisition effectiveness and channel quality. To ensure the accuracy of the calculation results, the computing engine must perform sorting and deduplication operations, materializing all upstream data through Flink State. However, State is an internal Black box that we cannot see or touch, making it extremely difficult to modify jobs or troubleshoot issues.


// TODO Image Placeholder


## 2.3 Difficulties in Maintenance of Large State Jobs

In e-commerce scenarios, order attribution is the core business logic. In the real-time data warehouse, it often requires the use of Dual Stream Join (click stream, transaction stream) + Flink State (24H) to implement the business logic of same-day transaction attribution. The resulting problem is the extremely large Flink State (for a single job **up to 100TB),** which includes operations such as sorting and Dual Stream Join.


// TODO Image Placeholder


During a Flink job, requires State to maintain intermediate result sets. When the execution plan verification succeeds after modifying the job, it starts from the latest state. As long as the execution plan verification fails, it has to **reinitialize State from 0 clock** , which is time-consuming and labor-intensive. Moreover, each time data is consumed, both Sort State and Join State are updated. Among them, the Sort State of the sorting operator can reach **90TB** , and the Join State can be as high as **10TB** . The huge state brings many problems, including high cost, poor job stability, Checkpoint timeout, slow restart and recovery, etc.


// TODO Image Placeholder


# 3. Fluss Core Competencies

## 3.1 What is Fluss?

> Fluss Official Documentation: [https://fluss.apache.org/](https://fluss.apache.org/)

> GitHub: [https://github.com/apache/fluss](https://github.com/apache/fluss)



// TODO Image Placeholder


Fluss, developed by the Flink team, is the next-generation stream storage for stream analysis, a stream storage built for real-time analysis. Fluss innovatively integrates columnar storage format and real-time update capabilities into stream storage, and is deeply integrated with Flink to help users build a streaming data warehouse with high throughput, low latency, and low cost. It has the following core features:

- Real-time read and write: Supports millisecond-level streaming read and write capabilities.

- Columnar pruning: Store real-time stream data in columnar format. Column pruning can improve read performance by 10 times and reduce network costs.

- Streaming Update: Supports real-time streaming updates for large-scale data. Supports partial column updates to achieve low-cost wide table stitching.

- CDC Subscription: Updates will generate a complete Change Log (CDC), and by consuming CDC through Flink streaming, real-time data flow across the whole-pipeline of the data warehouse can be achieved.

- Real-time point query: Supports high-performance primary key point query and can be used as a dimension table association for real-time processing links.

- Unified Lake and Stream: Seamlessly integrates Lakehouse and provides a real-time data layer for Lakehouse. This not only brings low-latency data to Lakehouse analytics but also endows stream storage with powerful analytical capabilities.


## 3.2 Table Type



// TODO Image Placeholder


**Type:** Divided into log tables and primary key tables. Log tables are columnar MQs that only support insert operations, while primary key tables can be updated according to the primary key and specified merge engine.

**Partition:** Divides data into smaller, more manageable subsets according to specified columns. Fluss supports more diverse partitioning strategies, such as Dynamic create partitions. For the latest documentation on partitioning, please refer to: [https://fluss.apache.org/docs/table-design/data-distribution/partitioning/](https://fluss.apache.org/docs/table-design/data-distribution/partitioning/) . Note that the partition type must be of String type and can be defined via the following SQL:

```SQL
CREATE TABLE temp(
  dt STRING
) PARTITIONED BY (dt)
WITH (
  'table.auto-partition.num-precreate' = '2' -- Create 2 partitions in advance
  ,'table.auto-partition.num-retention' = '2' -- Keep the first 2 partitions
  ,'table.auto-partition.enabled' = 'true' -- Automatic partitioning
);
```

**Bucket:** The smallest unit of read and write operations. For a primary key table, the bucket to which each piece of data belongs is determined based on the hash value of the primary key of each piece of data. For a log table, the configuration of column hashing can be specified in the with parameter when creating the table; otherwise, it will be randomly scattered.

### 3.2.1 Log Table

The log table is a commonly used table in Fluss, which writes data in the order of writing, only supports insert operations, and does not support update/delete operations, similar to MQ systems such as Kafka and TT. For the log tables, currently most of the data will be uploaded to a remote location, and only a portion of the data will be stored locally. For example, a log table has 128 buckets, only 128 \* 2 (number of segments retained locally) \* 1 (size of one segment) \* 3 (number of replicas) GB = 768 GB will be stored locally. The table creation statement is as follows:

```SQL
CREATE TABLE `temp` (
  second_timestamp  STRING
  ,pk               STRING
  ,assist           STRING
  ,user_name        STRING
)
with (
  'bucket.num' = '256' -- Number of buckets
  ,'table.log.ttl' = '2d' -- TTL setting, default 7 days
  ,'table.log.arrow.compression.type' = 'ZSTD' -- Compression mode, currently added by default
  ,'client.writer.bucket.no-key-assigner' = 'sticky' -- Bucket mode
);
```

In Fluss, the log table is stored in Apache Arrow columnar format by default. This format stores data column by column rather than row by row, thereby enabling **column pruning**. This ensures that only the required columns are consumed during real-time consumption, thereby reducing IO overhead, improving performance, and reducing resource usage.

### 3.2.2 PrimaryKey Table

Compared to the log table, the primary key table supports insert, update, and delete operations, and different merge methods are implemented by specifying the Merge Engine. It should be noted that `bucket.key` and `partitioned.key` need to be subsets of `primary.key`, and if this KV table is used for DeltaJoin, `bucket.key` needs to be a prefix of `primary.key`. The table creation statement is as follows:

```SQL
CREATE TABLE temp (
  ,pk               STRING
  ,user_name        STRING
  ,item_id          STRING
  ,event_time       BIGINT
  ,dt               STRING
  ,PRIMARY KEY (user_name,item_id,pk,dt) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
  'bucket.num' = '512'
  ,'bucket.key' = 'user_name,item_id'
  ,'table.auto-partition.enabled' = 'true'
  ,'table.auto-partition.time-unit' = 'day'
  ,'table.log.ttl' = '1d' -- binlog保留1天
  ,'table.merge-engine'='versioned' -- 拿最后一条数据
  ,'table.merge-engine.versioned.ver-column' = 'event_time' -- 排序字段
  ,'table.auto-partition.num-precreate' = '2' -- 提前创建2个分区
  ,'table.auto-partition.num-retention' = '2' -- 保留前2个分区
  ,'table.log.arrow.compression.type' = 'ZSTD'
)
;
```

Among them, merge-engine supports versioned and first\_row; after using versioned, it is necessary to limit the ver-column for sorting, currently only supporting types such as INT, BIGINT, and TIMESTAMP, and not supporting STRING; after using first\_row, it only supports retaining the first record of each primary key and does not support sorting by column.

## 3.3 Core Functions

### 3.3.1 Tailorable Columnar Storage

Fluss is a column-based streaming storage, with its underlying file storage adopting the Apache Arrow IPC streaming format. This enables Fluss to achieve efficient column pruning while maintaining millisecond-level streaming read and write capabilities.

A key advantage of Fluss is that column pruning is performed at the server level, and only the necessary columns are transferred to the Client. This architecture not only improves performance but also reduces network costs and resource consumption.


// TODO Image Placeholder


### 3.3.2 Message Queue of KV Storage

The core of Fluss's KV storage is built on top of the log Table, and a Key-Value (KV) index is constructed on the log. The KV index is implemented using an LSM tree, supporting large-scale real-time updates and partial updates, enabling efficient construction of wide tables. Additionally, the Change Log generated by KV can be directly read by Flink without additional deduplication costs, thus saving a significant amount of computing resources.

Fluss's built-in KV index supports high-performance primary key lookups. Users can also use Fluss for direct data exploration, including queries with operations such as LIMIT and COUNT, thus easily debugging data in Fluss.


// TODO Image Placeholder


### 3.3.3 Dual-Stream Join → Delta Join

In Flink, Dual-Stream Join is a very fundamental function, often used to build wide tables. However, it is also a function that often gives developers headaches. This is because Dual-Stream Join needs to maintain the full amount of upstream data in State, which results in its state usually being very large. This brings about many problems, including high costs, unstable jobs, Checkpoint timeouts, slow restart recovery, and so on.

Fluss has developed a brand-new Flink join operator implementation called Delta Join, which fully leverages Fluss's streaming read and Prefix Lookup capabilities. Delta Join can be simply understood as "**dual-sided driven dimension table join**". When data arrives on the left side, it performs a point query on the right table based on the Join Key; when data arrives on the right side, it performs a point query on the left table based on the Join Key. Throughout the process, it does not require state like a dimension table join, but implements semantics similar to a Dual-Stream Join, meaning that any data update on either side will trigger an update to the associated results.


// TODO Image Placeholder


### 3.3.4 Lake-Stream Integration

In the Kappa architecture, due to differences in production pipelines, data is stored separately in streams and lakes, resulting in cost waste. At the same time, additional data services need to be defined to unify data consumption. The goal of lake-stream integration is to enable "lake data" and "stream data" to be stored, managed, and consumed as a unified whole, thereby avoiding data redundancy and metadata inconsistency issues.


// TODO Image Placeholder


Additionally, Fluss provides Union Read to **fully manage** the unified data service required by the Kappa architecture. Fluss maintains a Compaction Service, which automatically converts Fluss data into lake storage format and ensures the consistency of lake-stream metadata. In addition, Fluss also provides partition alignment and bucket alignment mechanisms to ensure consistent distribution of lake-stream data. This allows for the direct conversion of Arrow files to Parquet files without the need to introduce network shuffling during the process of transferring to the lake.

# 4. Architecture Evolution and Capability Implementation

## 4.1 Architecture Evolution

Fluss innovatively integrates columnar storage format and real-time update capabilities into stream storage and deeply integrates with Flink. Based on Fluss's core capabilities, we further enhance the real-time architecture, building a high-throughput, low-latency, and low-cost lakehouse through Fluss. The following takes the typical upgrade scenario of the Babel Tower as an example to introduce the implementation practice of Fluss.

### 4.1.1 Evolution of Regular Jobs

// TODO Image Placeholder

The above is the architecture before and after the job upgrade. For routine jobs such as `Source -> ETL cleaning -> Sink`, since the message queue is row-based storage, when consuming, Flink first loads the entire row of data into memory and then filters the required columns, resulting in a significant waste of Source IO. After upgrading to Fluss, due to the columnar storage at the bottom of Fluss, column pruning in Fluss is performed at the server level, which means that the data sent to the Client has already been pruned, thus saving a large amount of network costs.

### 4.1.2 Evolution of Sorting Jobs



// TODO Image Placeholder


In Flink, the implementation of sorting relies on Flink explicitly computing and using State to store the intermediate state of data. This model incurs significant business overhead and extremely low business reusability. The introduction of Fluss pushes this computation and storage down to the Sink side, and in conjunction with the Fluss Merge Engine, implements different deduplication methods for KV tables. Currently, it supports **FirstRow Merge Engine** (the first row) and **Versioned Merge Engine** (the latest row). The Changelog generated during deduplication can be directly read by Flink streams, saving a large amount of computing resources and enabling the rapid reuse and implementation of data services.

### 4.1.3 Evolution of Dual-Stream Join Jobs



// TODO Image Placeholder


After the Fluss remodeling, the Dual-Stream Join of the Babel Tower transaction attributed job has been enhanced with the following upgrade points:

- Column pruning is truly pre-positioned to Source consumption, avoiding IO consumption of useless columns.

- Introduce Fluss KV table + Merge Engine to implement data sorting and eliminate the dependency on Flink sorting State.

- Refactor the Dual-Stream Join into FlussDeltaJoin, using stream reading + index point query, and externalize the Flink Dual-Stream JoinState.

- A comprehensive comparison between traditional Dual-Stream Join and the new Fluss-based architecture reveals the following advantages and disadvantages of the two:




// TODO Image Placeholder


### 4.1.4 Evolution of Lake Jobs



// TODO Image Placeholder


Under the Fluss lake-stream integrated architecture, Fluss provides a **fully managed** unified data service. Fluss and Paimon store stream and lake data respectively, output a Catalog to the computing engine (such as Flink), and the data is output externally in the form of a unified table. Consumers can directly access the data in Fluss and lake storage in the form of Union Read.

## 4.2 Implementation of Core Competencies

### 4.2.1 Column Pruning Capability

In the job of consuming message queues, consumers typically only consume a portion of the data, but Flink jobs still need to read data from all columns, resulting in significant waste in Flink Source IO. Fundamentally, existing message queues are all row-based storage, and for scenarios that need to process large-scale data, the efficiency of row-based storage format appears insufficient. The underlying storage needs to have powerful Data Skipping capabilities and support features such as column pruning. In this case, Fluss with columnar storage is clearly more suitable.


// TODO Image Placeholder


In our real-time data warehouse, 70% of jobs only consume partial columns of Source. Taking the Babel Tower recommendation clicks job as an example, out of the 43 fields in Source, we only need 13. Additional operator resources are required to trim the entire row of data, wasting more than 20% of IO resources. After using Fluss, it directly consumes the required columns, avoiding additional IO waste and reducing the additional resources brought by Flink column pruning operators. To date, multiple core jobs in the Taotian Search and Recommendation domain have already launched Fluss and have been verified during the Taotian 618 promotion.


### 4.2.2 Real-time Data Profiling

**Data Point Query**

Whether troubleshooting issues or conducting data exploration, data queries are necessary. However, the message queue only supports sampling queries in the interface and queries of synchronized data in additional storage. In sampling queries, it is not possible to query specified data; only a batch of output can be retrieved for display and inspection. Using the method of synchronizing additional storage, on the other hand, incurs minute-level latency as well as additional storage and computational costs.


// TODO Image Placeholder


In Fluss KV Table, a KV index is built, so it can support high-performance primary key point queries, directly probe Fluss data through point query statements, and also support query functions such as LIMIT and COUNT to meet daily Data Profiling requirements. An example is as follows:


// TODO Image Placeholder


**Flink State Query**

Flink's State mechanism (such as KeyedState or OperatorState) provides efficient state management capabilities, but its internal implementation is a "Black box" to developers. Developers cannot directly query, analyze, or debug the data in State, resulting in a strong coupling between business logic and state management, making it difficult to dynamically adjust or expand. When data anomalies occur, developers can only infer the content in State from the result data, unable to directly access the specific data in State, leading to high costs for troubleshooting.


// TODO Image Placeholder


We externalize Flink State into Fluss, such as states for duak-stream join, data deduplication, etc. Based on Fluss's KV index, we provide State exploration capabilities, white-box the internal data of State, and efficiently locate issues.


// TODO Image Placeholder


### 4.2.3 Merge Engine + Delta Join

In the current real-time data warehouse, the transaction attribution task is a job that heavily relies on State, with State **reaching up to 100TB** , which includes operations such as sorting and Dual-Stream Join. After consuming TT data, we first perform data sorting and then conduct a Dual-Stream Join to attribute order data.


// TODO Image Placeholder


As shown in the figure, in the first attribution logic implementation of the attribution job, the State of the sorting operator is as high as **90TB** , and that of the Dual-Stream Join operator is **10TB** . Large State jobs bring many problems, including high costs, job instability, long CP time, etc.


**Sorting optimization, Merge Engine**


// TODO Image Placeholder


In the implementation of the Merge Engine, it mainly relies on Fluss's KV table. The Changelog generated by KV can be read by Flink streams without additional deduplication operations, saving Flink's computing resources and achieving business reuse of data. Through Fluss's KV table, the sorting logic in our jobs is implemented, and there is no longer a need to maintain the state of the sorting operator in the jobs.


**Join Optimization, Dual Sides Driving**


// TODO Image Placeholder


The jobs of Delta Join are as described above. The original job, after consuming data, sorts it according to business requirements, performs Dual-Stream Join after sorting, and saves the state for 24 hours. This results in issues such as long CP time, poor job maintainability, and the need to rerun when modifying the job state. In Fluss, data sorting is completed through the Merge Engine of the KV table, and through Delta Join, the job and state are decoupled, eliminating the need to rerun the state when modifying the job, making the state data queryable, and improving flexibility.

We use the transaction attribution job as an example. It should be noted that both sides of DeltaJoin need to be KV tables.

```SQL
-- Create left table
CREATE TABLE `fluss`.`sr_ds`.`dpv_versioned_merge`(
   pk              VARCHAR
  ,user_name       VARCHAR
  ,item_id         VARCHAR
  ,step_no         VARCHAR
  ,event_time      BIGINT
  ,dt              VARCHAR
  ,PRIMARY KEY (user_name,item_id,step_no,dt) NOT ENFORCED
)
PARTITIONED BY (dt) WITH (
      'bucket.num' = '512' 
      ,'bucket.key' = 'user_name,item_id'
      ,'table.auto-partition.enabled' = 'true'
      ,'table.auto-partition.time-unit' = 'day'
      ,'table.merge-engine'='versioned' -- Take the last piece of data
      ,'table.merge-engine.versioned.ver-column' = 'event_time' -- Sort field
      ,'table.auto-partition.num-precreate' = '2'
      ,'table.auto-partition.num-retention' = '2'
      ,'table.log.arrow.compression.type' = 'ZSTD'
);
-- Create right table
CREATE TABLE `fluss`.`sr_ds`.`deal_kv`
(
  ,pk               VARCHAR
  ,user_name        VARCHAR
  ,item_id          VARCHAR
  ,event_time       bigint
  ,dt               VARCHAR
  ,PRIMARY KEY (user_name,item_id,pk,dt) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
  'bucket.num' = '48'
  ,'bucket.key' = 'user_name,item_id'
  ,'table.auto-partition.enabled' = 'true'
  ,'table.auto-partition.time-unit' = 'day'
  ,'table.merge-engine' = 'first_row'
  ,'table.auto-partition.num-precreate' = '2'
  ,'table.auto-partition.num-retention' = '2'
  ,'table.log.arrow.compression.type' = 'ZSTD'
)
;
-- Join logic
select * from dpv_versioned_merge t1
join
select * from deal_kv t2
  on t1.dt = t2.dt 
  and t1.user_name = t2.user_name 
  and t1.item_id = t2.item_id
```

Some operations are shown in the above code. After migrating from the Dual-Stream Join to Merge Engine + Delta Join, large states were successfully reduced, making the job run more stably and eliminating CP timeouts. Meanwhile, the actual usage of CPU and Memory also decreased.


In addition to resource reduction and performance improvement, there is also an enhancement in flexibility for our benefits. The state of traditional stream-to-stream connections is tightly coupled with Flink jobs, like an opaque "black box". When the job is modified, it is found that the historical resource plan is incompatible with the current job, and the State can only be rerun from scratch, which is time-consuming and labor-intensive. After using Delta Join, it is equivalent to decoupling the state from the job, so modifying the job does not require rerunning the State. Moreover, all data is stored in Fluss, making it queryable and analyzable, thus improving business flexibility and development efficiency.


// TODO Image Placeholder


Meanwhile, Fluss maintains a Compaction Service to synchronize Fluss data to Paimon, with the latest data stored in Fluss and historical data in Paimon. Flink can support Union Read, which combines the data in Fluss and Paimon to achieve second-level freshness analysis.


// TODO Image Placeholder


In addition, Data Join also addresses the scenario of backtracking data. In the case of Dual-Stream Join, if a job modification causes the Operating Plan verification to fail, only data backfilling can be performed. During the Fluss backtracking process, the Paimon table archived by the unified lake-stream architecture combined with Flink Batch Join can be used to accelerate data backfilling.

### 4.2.4 Lake-Stream Evolution

Fluss provides high compatibility with Data lake warehouses. Through the underlying Service, it automatically converts Fluss data into Paimon format data, enabling real-time data to be ingested into the lake with a single click, while ensuring consistent data partitioning and bucketing on both the lake and stream sides.


// TODO Image Placeholder


After having **lake** and **stream** two levels of data, Fluss has the key characteristic of sharing data. Paimon stores long-period, minute-level latency data; Fluss stores short-period, millisecond-level latency data, and the data of both can be shared with each other.

When performing Fluss stream reads, Paimon can provide efficient backtracking capabilities as historical data. After backtracking to the current checkpoint, the system will automatically switch to stream storage to continue reading and ensure that no duplicate data is read. In batch query analysis, Fluss stream storage can supplement Paimon with real-time data, thereby enabling analysis with second-level freshness. This feature is called Union Read.

# 5. Benefits and Summary

After a period of in-depth research, comprehensive testing, and smooth launch of Fluss, we successfully completed the evolution of the technical architecture based on the Fluss link. Among them, we conducted comprehensive tests on core capabilities such as Fluss column pruning and Delta Join to ensure the stability and performance of the link met the standards.

## 5.1 Performance Test

### 5.1.1 Read and Write Performance

We tested the read-write and column pruning capabilities of Fluss. Among them, in terms of read-write performance, we conducted tests with the same traffic. While maintaining the input **RPS at 800 w/s** and the output **RPS at 44 w/s** , we compared the actual CPU and Memory usage of Flink jobs. The details are as follows:


// TODO Image Placeholder


### 5.1.2 Column Pruning Performance

Regarding column pruning capabilities, we also tested TT, Fluss, and different numbers of columns in Fluss to explore the consumption of CPU, Memory, and IO. With the input **RPS maintained at 25 w/s** , the specific test results are as follows:


// TODO Image Placeholder


At this point, we will find a problem: as the number of columns decreases (a total of 13 columns out of 43 columns are consumed), IO does not show a linear decrease. After verification, the storage of each column in the source is not evenly distributed, and the storage of some required columns **accounts for 72%** . It can be seen that the input IO traffic **decreases by 20%** , which is consistent with the expected proportion of the storage of the read columns.

### 5.1.3 Delta Join Performance

We also tested and compared the resources and performance of the above **Dual-Stream Join and Fluss Delta Join** , and under the condition of maintaining the **input RPS of the left table at 68,000/s** and the **input RPS of the right table at 1,700/s** , all jobs ran for 24 hours.


// TODO Image Placeholder


After actual testing, after migrating from the Dual-Stream Join to Merge Engine + Delta Join, it successfully **reduced 95TB** of large state, making the job run more stably and significantly shortening the CP time. Meanwhile, the actual usage of CPU and Memory also **decreased by more than 80%** .

### 5.1.4 Data Backtracking Performance

Since Delta Join **does not need to maintain state** , it can use batch mode **Batch Join** to catch up. After catching up, it switches back to stream mode Delta Join. During this process, a small amount of data will be reprocessed, and the end-to-end consistency is ensured through the idempotence update mechanism of the result table. We **performed a test comparison on the data catch-up of the above** Dual-Stream Join and Batch Join **for one day (24H).**


// TODO Image Placeholder


After actual testing, the batch mode Batch Join backtracking for one day (24H) takes **70%+ less time** compared to the Dual-Stream Join backtracking.

## 5.2 Summary

Fluss has core features such as columnar pruning, streaming updates, real-time point queries, and lake-stream integration. At the same time, it innovatively integrates columnar storage format and real-time update capabilities into stream storage, and deeply integrates with Flink and Paimon to build a lake warehouse with high throughput, low latency, and low cost. Just as the original intention of Fluss: **Real-time stream storage for analytics.**

After nearly three months of exploration, Fluss has been implemented in **Taotian's core search and recommendation scenarios** , built a unified lake-stream AB data warehouse, and continuously served internal business through Taobao's AB Experiment Platform (The Babel Tower). In practical applications, the following results have been achieved:

- **Scenarios are widely implemented:** covering core Taotian business such as search and recommendation, and successfully passing the test of Taotian's 618 Grand Promotion **,** with peak traffic reaching tens of millions and average latency **within 1 second** .

- **Column Pruning Resource Optimization** : The column pruning feature has been implemented in all Fluss real-time jobs. When the consumption of columnar storage accounts for 75% of the source data, the average IO **is reduced by 25%** , and the average job resource consumption **is reduced by 30%** .

- **Big State job optimization:** Taking transaction attribution job as an example, the Delta Join + Merge Engine is applied to reconstruct the first and last attribution. State is externally implemented, and the actual usage of CPU and Memory is **reduced by 80% +** . The state of Flink is **decoupled** from the job, and the **100TB +** big state is successfully reduced, making the job more stable.

- **Data Profiling:** Implemented capabilities including message queue profiling and State profiling, supports query operators such as LIMIT and COUNT, achieved white-boxing of State for sorting and Dual-Stream Join, with higher flexibility and more efficient problem location.


# 6. Planning


// TODO Image Placeholder


In the upcoming work, we will develop a new generation of lakehouse data architecture and continue to explore in Data + AI scenarios.

- **Scenario Inclusiveness:** In the second-level data warehouse, switch the whole-pipeline and all scenarios of the consumption message queue to the Fluss pipeline. Based on the Fluss lake-stream capabilities, provide corresponding Paimon minute-level data.

- **AI Empowerment:** Attempt to implement scenarios of MultiModal Machine Learning data pipelines, Agents, and model pre-training, and add AI-enhanced analytics.

- **Capability Exploration:** Explore more Fluss capabilities and empower business operations, optimizing more jobs through Fluss Aggregate Merge Engine and Partial Update capabilities.


# References

[1] [Fluss Official Documentation](https://fluss.apache.org/)

[2] [Fluss: Next-Generation Stream Storage Designed for Real-Time Analysis](https://mp.weixin.qq.com/s?__biz=MzU3Mzg4OTMyNQ==&mid=2247512207&idx=1&sn=e25320a020d7b20e25be8fd614e9f46b&chksm=fd383ecdca4fb7dbf9a37e9c3840699ba8b1b2c57868db028f995c3bc7bcf6a0340e396d87c4&scene=178&cur_album_id=3764887437743669257&search_click_id=#rd)
