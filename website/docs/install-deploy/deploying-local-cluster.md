---
title: "Deploying Local Cluster"
sidebar_position: 2
---

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

# Deploying Local Cluster

This page provides instructions on how to deploy a *local cluster* (on one machine, but in separate processes) for Fluss.

## Requirements

Fluss runs on all *UNIX-like environments*, e.g. **Linux**, **Mac OS X**.
Before you start to setup the system, make sure you have the following software installed on your test machine:

- **Java 17** or higher (Java 8 and Java 11 are not recommended)

If your cluster does not fulfill these software requirements you will need to install/upgrade it.

### `JAVA_HOME` Configuration

Fluss requires the `JAVA_HOME` environment variable to be set on your
test machine and point to the directory of your Java installation.

## Fluss Setup

Go to the [downloads page](/downloads) and download the latest Fluss release. Make sure to pick the Fluss
package **matching your Java version**. After downloading the latest release, extract it:

```shell
tar -xzf fluss-$FLUSS_VERSION$-bin.tgz
cd fluss-$FLUSS_VERSION$/
```

## Starting Fluss Local Cluster

You can start Fluss local cluster by running the following command:
```shell
./bin/local-cluster.sh start
```

After that, the Fluss local cluster is started.

## Interacting with Fluss

After Fluss local cluster is started, you can use **Fluss Client** (Currently, only support Flink Sql Client) to interact with Fluss.
The following subsections will show you how to use Flink Sql Client to interact with Fluss.

### Flink SQL Client

Using Flink SQL Client to interact with Fluss.

#### Preparation

You can start a Flink standalone cluster refer to [Flink Environment Preparation](engine-flink/getting-started.md#preparation-when-using-flink-sql-client)

**Note**: Make sure the [Fluss connector jar](/downloads/) already has copied to the `lib` directory of your Flink home.

#### Add catalog

In Flink SQL client, a catalog is created and named by executing the following query:
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'localhost:9123'
);
```

#### Do more with Fluss

After the catalog is created, you can use Flink SQL Client to do more with Fluss, for example, create a table, insert data, query data, etc.
More details please refer to [Flink Getting started](engine-flink/getting-started.md)