---
sidebar_position: 3
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

# Lakehouse Storage

Lakehouse represents a new, open architecture that combines the best elements of data lakes and data warehouses.
Lakehouse combines data lake scalability and cost-effectiveness with data warehouse reliability and performance.

Fluss leverages the well-known Lakehouse storage solutions like Apache Paimon, Apache Iceberg, Apache Hudi, Delta Lake as
the tiered storage layer. Currently, only Apache Paimon is supported, but more kinds of Lakehouse storage support are on the way.

Fluss's datalake tiering service will compact Fluss's data to the Lakehouse storage continuously. The data in Lakehouse storage can be read both by Fluss's client in a streaming manner and accessed directly
by external systems such as Flink, Spark, StarRocks and others. With data tiered in Lakehouse storage, Fluss
can gain much storage cost reduction and analytics performance improvement.


## Enable Lakehouse Storage

Lakehouse Storage disabled by default, you must enable it manually.

### Lakehouse Storage Cluster Configurations
First, you must configure the lakehouse storage in `server.yaml`. Take Paimon
as an example, you must configure the following configurations:
```yaml
datalake.format: paimon

# the catalog config about Paimon, assuming using Filesystem catalog
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon_data_warehouse
```

### Start The Datalake Tiering Service
Then, you must start the datalake tiering service to compact Fluss's data to the lakehouse storage.
To start the datalake tiering service, you must have a Flink cluster running since Fluss currently only supports Flink as a tiering service backend.

You can use the following commands to start the datalake tiering service:
```shell
# change directory to Fluss 
cd $FLUSS_HOME

# start the tiering service, assuming rest endpoint is localhost:8081
./bin/lakehouse.sh -D flink.rest.address=localhost -D flink.rest.port=8081 
```

**Note:**
- `flink.rest.address` and `flink.rest.port` are the Flink cluster's rest endpoint, you may need to change it according to your Flink cluster's configuration.
- The datalake tiering service is actual a flink job, you can set the Flink configuration in `-D` arguments while starting the datalake tiering service, There are some example commands for reference below.
```shell
# If want to set the checkpoint interval to 10s, you can use the following command to start the datalake tiering service
./bin/lakehouse.sh -D flink.rest.address=localhost -D flink.rest.port=8081 -D flink.execution.checkpointing.interval=10s

# By default, datalake tiering service synchronizes all the tables with datalake enabled to Lakehouse Storage.
# To distribute the workload of the datalake tiering service through multiple Flink jobs, 
# you can specify the "database" parameter to synchronize only the datalake enabled tables in the specific database.
./bin/lakehouse.sh -D flink.rest.address=localhost -D flink.rest.port=8081 -D database=fluss_\\w+
```

### Enable Lakehouse Storage Per Table
To enable lakehouse storage for a table, the table must be created with the option `'table.datalake.enabled' = 'true'`.