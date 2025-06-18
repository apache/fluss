---
title: Metric Reporters
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

# Metric Reporters

Fluss allows reporting [metrics](monitor-metrics.md) to external system. 
Metrics can be exposed to an external system by configuring one or several reporters in `conf/server.yaml`. These 
reporters will be instantiated on each CoordinatorServer and TabletServers when they started.

Example reporter configuration that specifies multiple reporters:

```yaml
metrics.reporters: jmx,prometheus
```

## Push vs. Pull

Metrics are exported either via pushes or pulls.

Push-based reporters usually implement the `Scheduled` interface and periodically send a summary of current metrics to an external system.

Pull-based reporters are queried from an external system instead.

## Reporters

The following sections list the supported reporters currently.

### JMX

Type: pull

Parameters:

- `port` - (optional) the port on which JMX listens for connections.
  In order to be able to run several instances of the reporter on one host (e.g. when one TabletServer is co-located with the CoordinatorServer) it is advisable to use a port range like `9250-9260`.
  When a range is specified the actual port is shown in the relevant server log.
  If this setting is set, Fluss will start an extra JMX connector for the given port/range.
  Metrics are always available on the default local JMX interface.

Example configuration:

```yaml
metrics.reporters: jmx
metrics.reporter.jmx.port: 9250-9260
```

Metrics exposed through JMX are identified by a domain and a list of key-properties, which together form the object name.

The domain always begins with `com.alibaba.fluss` followed by a generalized metric identifier.
An example for such a domain would be `com.alibaba.fluss.tabletserver.replicaCount`.

The key-property list contains the values for all variables, that are associated
with a given metric.
An example for such a list would be `cluster_id=fluss1,host=localhost,server_id=1`.

The domain thus identifies a metric class, while the key-property list identifies one (or multiple) instances of that metric.

### Prometheus

Type: pull

Parameters:

- `metrics.reporter.prometheus.port` - (optional) the port the Prometheus exporter listens on, defaults to [9249](https://github.com/prometheus/prometheus/wiki/Default-port-allocations). In order to be able to run several instances of the reporter on one host (e.g. when one TabletServer is co-located with the CoordinatorServer) it is advisable to use a port range like `9250-9260`.

Example configuration:

```yaml
metrics.reporters: prometheus
metrics.reporter.prometheus.port: 9250
```

Fluss metric types are mapped to Prometheus metric types as follows:

| Fluss     | Prometheus | Note                                     |
| --------- |------------|------------------------------------------|
| Counter   | Gauge      |Prometheus counters cannot be decremented.|
| Gauge     | Gauge      |Only numbers and booleans are supported.  |
| Histogram | Summary    |Quantiles .5, .75, .95, .98, .99 and .999 |
| Meter     | Gauge      |The gauge exports the meter's rate.       |
