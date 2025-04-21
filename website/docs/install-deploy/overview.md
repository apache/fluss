---
sidebar_label: "Overview"
sidebar_position: 1
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

# Overview

Below, we provide an overview of the key components of a Fluss cluster, detailing their functionalities and implementations. Additionally, we will introduce the various deployment methods available for Fluss.

## Overview and Reference Architecture

The figure below shows the building blocks of Fluss clusters:

<img width="1200px" src={require('../assets/deployment_overview.png').default} />



When deploying Fluss, there are often multiple options available for each building block.
We have listed them in the table below the figure.


<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" width="250">Component</th>
      <th class="text-left" width="600">Purpose</th>
      <th class="text-left" width="300">Implementations</th>
    </tr>
   </thead>
   <tbody>
        <tr>
            <td>Fluss Client</td>
            <td>
                <p>
                    The Fluss Client is the entry point for users to interact with Fluss Cluster. It is responsible for 
                    managing Fluss Cluster like:
                </p>
                <ul>
                    <li> Admin operation: like create or delete database/table etc</li>
                    <li>Table operation: like write, read, delete data</li>
                </ul>
            </td>
            <td>
                <ul>
                    <li>[Flink Connector](engine-flink/getting-started.md)</li>
                </ul>
            </td>
        </tr>
        <tr>
            <td>CoordinatorServer</td>
            <td>
                <p>
                CoordinatorServer is the name of the central work coordination component of Fluss. 
                The coordinator server is responsible to:
                </p>
                <ul>
                    <li>Manage the TabletServer</li>
                    <li>Manage the metadata</li>
                    <li>Coordinate the whole cluster, e.g. data re-balance, recover data when tablet servers down</li>
                </ul>
            </td>
            <td rowspan="2">
                <ul>
                    <li>[Local Cluster](install-deploy/deploying-local-cluster.md)</li>
                    <li>[Distributed Cluster](install-deploy/deploying-distributed-cluster.md)</li>
                    <li>[Docker run / Docker compose](install-deploy/deploying-with-docker.md)</li>
                </ul>
            </td>
        </tr>
        <tr>
            <td>TabletServer</td>
            <td>
                <p>
                TabletServers are the actual node to manage and store data.
                </p>
            </td>
        </tr>
        <tr>
            <td colspan="3" style={{ textAlign: "center" }}>
                <b>External Components</b>
            </td>
        </tr>
            <tr>
                <td>ZooKeeper</td>
                    <td>
                        :::warning
                        Zookeeper will be removed to simplify deployment in the near future. For more details, please checkout [Roadmap](/roadmap/).
                        :::
                        <p>
                        Fluss leverages ZooKeeper for distributed coordination between all running CoordinatorServer instances and for metadata management.
                        </p>
                    </td>
                    <td>
                        <ul>
                            <li><a href="https://zookeeper.apache.org/">Zookeeper</a></li>
                        </ul>
                    </td>
                </tr>
            <tr>
            <td>Remote Storage (optional)</td>
            <td>
                Fluss uses file systems as remote storage to store snapshots for Primary-Key Table and store tiered log segments for Log Table.
            </td>
            <td>
            <li>[HDFS](maintenance/filesystems/hdfs.md)</li>
            <li>[Aliyun OSS](maintenance/filesystems/oss.md)</li>
            <li>[Amazon S3](maintenance/filesystems/s3.md)</li>
            </td>
        </tr>
        <tr>
            <td>Lakehouse Storage (optional)</td>
            <td>
               Fluss's DataLake Tiering Service will continuously compact Fluss's Arrow files into Parquet/ORC files in open lake format.
               The data in Lakehouse storage can be read both by Fluss's client in a Union Read manner and accessed directly
               by query engines such as Flink, Spark, StarRocks, Trino.
            </td>
            <td>
            <li>[Paimon](maintenance/tiered-storage/lakehouse-storage.md)</li>
            <li>[Iceberg (Roadmap)](/roadmap/)</li>
            </td>
        </tr>
        <tr>
            <td>Metrics Storage (optional)</td>
            <td>
                CoordinatorServer/TabletServer report internal metrics and Fluss client (e.g., connector in Flink jobs) can report additional, client specific metrics as well.
            </td>
            <td>
               <li>[JMX](maintenance/observability/metric-reporters.md#jmx)</li>
               <li>[Prometheus](maintenance/observability/metric-reporters.md#prometheus)</li>
            </td>
        </tr>
    </tbody>
</table>

## How to deploy Fluss

Fluss can be deployed in three different ways:
- [Local Cluster](install-deploy/deploying-local-cluster.md)
- [Distributed Cluster](install-deploy/deploying-distributed-cluster.md)
- [Docker run/ Docker compose](install-deploy/deploying-with-docker.md)

**NOTE**:
- Local Cluster is for testing purpose only.