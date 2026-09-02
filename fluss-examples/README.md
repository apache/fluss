<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Fluss Examples

This is a curated list of examples that showcase event streaming using Apache Pulsar.

# Prerequisites

- Docker + Compose installed on your machine.

```bash
# cd fluss-examples
docker compose up
```

## Full list of Java Client Basic Examples

- [x][Admin](clientjava-admin/)
- [x][Basic Log Table Write and Read](clientjava-basic-log-table/)
- [x][Basic PrimaryKey Table Write and Lookup](clientjava-basic-primarykey-table/)
- [x][PrimaryKey Mult-Keys Table Write and PrefixLookup](clientjava-keys-primarykey-table/)
- [x][Schema Definitions](examples-commons/)
- [x][Table Bucketing](clientjava-keys-primarykey-table/)
- [ ][Table Partitions]()

### Recommendation for next examples

Fluss is designed to address the demands of real-time analytics with the following key capabilities:

- [x] Real-Time Stream Reading and Writing: Supports millisecond-level end-to-end latency.
- [x] Streaming Updates: Enables low-latency updates to data streams.
- [x] Real-Time Lookup Queries: Facilitates instant lookup queries on primary keys.
- [ ] Columnar Stream: Optimizes storage and query efficiency.
- [ ] Changelog Generation: Supports changelog generation and consumption.
- [ ] Streaming & Lakehouse Unification: Seamlessly integrates streaming and lakehouse storage for unified data
  processing.

## Running the Environment

Execute the docker compose to spin up a Fluss cluster, but create the Fluss network first:

```bash
docker network create fluss-net
docker compose up
```

## Building examples

You can build the examples using Maven. Navigate to the `fluss-examples` directory and run:

```bash
mvn spotless:apply

mvn clean package
```

## Runing the Examples

Enter into example directory and run the example using the following command:

```bash
cd clientjava-multkeys-primarykey-table
java -jar target/clientjava-multkeys-primarykey-table-0.8-SNAPSHOT.jar
```




