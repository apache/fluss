---
title: Cluster Monitoring and Observability
sidebar_position: 4
---

# Cluster Monitoring and Observability

This guide will show you how to set up a monitoring/observability stack for Fluss. 
It is based on the [Flink Quickstart Guide](flink.md), but the principles for other query engines are the same.

We provide instructions for two different setups:

- [Cluster Monitoring (Metrics, Logs) with Prometheus and Loki](#cluster-monitoring-metrics-logs-with-prometheus-and-loki)
- [Cluster Monitoring (Metrics, Logs) with OpenTelemetry](#cluster-monitoring-metrics-and-logs-with-opentelemetry)

The setups primarily differ in the way telemetry data is reported to the respective telemetry backends. 
While the first setup is tightly coupled with the chosen stack ([Prometheus](https://prometheus.io/), [Loki](https://grafana.com/oss/loki/)) and directly integrates with the telemetry backends, [OpenTelemetry](https://opentelemetry.io/) is vendor-neutral and uses an intermediate collector that decouples Fluss from telemetry backends.

Both setups use [Grafana](https://grafana.com/oss/grafana/) as a visualization frontend. We provide 2 metric dashboards out-of-the-box:

- `Fluss – overview`: Selected metrics to get insights into the overall cluster status
- `Fluss – detail`: Majority of metrics listed in [metrics list](maintenance/observability/monitor-metrics.md#metrics-list)

All used components are publicly available as open source software.

:::note
- This guide aims at getting you up and running with a _minimal_ setup for cluster monitoring/observability. For production use, you need to adapt the setup accordingly, especially with respect to security-related configurations.
- Only Fluss cluster components are instrumented. If you want to instrument other parts of your setup, e.g., Flink, please refer the corresponding documentation.
- We highly encourage you to use [OpenTelemetry](https://opentelemetry.io/), as it is vendor-neutral and can scale to large deployments.
:::



## Preparation

In this section, you can find the common preparation steps that apply to all setups.

1. Download the <a href={ require("../assets/fluss-quickstart-observability.zip").default } target="_blank">observability quickstart configuration</a>.
2. Extract the ZIP archive in your working directory.
3. After extracting the archive, the contents of your working directory should be as follows.

```
├── docker-compose.yml              # Docker compose manifest from the Flink quickstart guide
└── fluss-quickstart-observability  # Downloaded and extracted ZIP archive (has to have the exact name shown)
    ├── grafana
    │   ├── grafana.ini
    │   └── provisioning
    │       ├── dashboards
    │       │   ├── default.yml
    │       │   └── fluss
                    ├── fluss-detail.json
    │       │       └── fluss-overview.json
    │       └── datatsources
    │           └── default.yml
    ├── opentelemetry
    │   ├── opentelemetry.yml
    │   └── opentelemetry-javaagent.properties
    ├── prometheus
    │   ├── prometheus-direct.yml
    │   └── prometheus-opentelemetry.yml
    └── slf4j
        ├── log4j-opentelemetry-console.properties
        └── logback-loki-console.xml
```



## Cluster Monitoring (Metrics, Logs) with Prometheus and Loki

This section will show you how to monitor your cluster with [Prometheus](https://prometheus.io/) (metric aggregation system) and [Loki](https://grafana.com/oss/loki/) (log aggregation system). 

1. First, you need to configure Fluss to expose logs to Loki. We will use [Loki4j](https://loki4j.github.io/loki-logback-appender/) which uses Logback as logging backend.
The container manifest below configures the Fluss image to use Logback and adds the Loki4j Logback appender to the classpath. Save it to a file named `fluss-slf4j-logback.Dockerfile` in your working directory.

```dockerfile
FROM apache/fluss:$FLUSS_DOCKER_VERSION$

# remove default logging backend from classpath and add logback to classpath
RUN rm -rf ${FLUSS_HOME}/lib/log4j-slf4j-impl-*.jar && \
    wget https://repo1.maven.org/maven2/ch/qos/logback/logback-classic/1.2.13/logback-classic-1.2.13.jar -P ${FLUSS_HOME}/lib/ && \
    wget https://repo1.maven.org/maven2/ch/qos/logback/logback-core/1.2.13/logback-core-1.2.13.jar -P ${FLUSS_HOME}/lib/

# add loki4j logback appender to classpath
RUN wget https://repo1.maven.org/maven2/com/github/loki4j/loki-logback-appender/1.4.2/loki-logback-appender-1.4.2.jar -P ${FLUSS_HOME}/lib/
```

:::note
Detailed configuration instructions for Fluss and Logback can be found [here](maintenance/observability/logging.md#configuring-logback).
:::

2. Next, you need to adapt the `docker-compose.yml` manifest and 

- build and use the new Fluss image manifest (`fluss-sfl4j-logback.Dockerfile`).
- configure Fluss to expose metrics via Prometheus.
- add the desired application name that should be used when displaying Fluss logs in Grafana as environment variable (`APP_NAME`).
- add containers for Prometheus, Loki and Grafana.
- mount the corresponding configuration files.

You can simply copy the manifest below into `docker-compose.yml`.

```yaml
services:
  #begin Fluss cluster
  coordinator-server:
    image: fluss-slf4j-logback:$FLUSS_DOCKER_VERSION$
    build:
      dockerfile: fluss-slf4j-logback.Dockerfile
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: /tmp/fluss/remote-data
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: /tmp/paimon
        metrics.reporters: prometheus
        metrics.reporter.prometheus.port: 9250
      - APP_NAME=coordinator-server
    volumes:
      - ./fluss-quickstart-observability/slf4j/logback-loki-console.xml:/opt/fluss/conf/logback-console.xml:ro
  tablet-server:
    image: fluss-slf4j-logback:$FLUSS_DOCKER_VERSION$
    build:
      dockerfile: fluss-slf4j-logback.Dockerfile
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://tablet-server:9123
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        kv.snapshot.interval: 0s
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: /tmp/paimon
        metrics.reporters: prometheus
        metrics.reporter.prometheus.port: 9250
      - APP_NAME=tablet-server
    volumes:
      - ./fluss-quickstart-observability/slf4j/logback-loki-console.xml:/opt/fluss/conf/logback-console.xml:ro
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  #end
  #begin Flink cluster
  jobmanager:
    image: apache/fluss-quickstart-flink:1.20-$FLUSS_DOCKER_VERSION$
    ports:
      - "8083:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - shared-tmpfs:/tmp/paimon
  taskmanager:
    image: apache/fluss-quickstart-flink:1.20-$FLUSS_DOCKER_VERSION$
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.framework.off-heap.size: 256m
    volumes:
      - shared-tmpfs:/tmp/paimon
  #end
  #begin monitoring
  prometheus:
    image: bitnami/prometheus:2.55.1-debian-12-r0
    ports:
      - "9092:9090"
    volumes:
      - ./fluss-quickstart-observability/prometheus/prometheus-direct.yml:/etc/prometheus/prometheus.yml:ro
  loki:
    image: grafana/loki:3.3.2
    ports:
      - "3102:3100"
  grafana:
    image:
      grafana/grafana:11.4.0
    ports:
      - "3002:3000"
    depends_on:
      - prometheus
      - loki
    volumes:
      - ./fluss-quickstart-observability/grafana:/etc/grafana:ro
  #end

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

3. Run

```shell
# note the --build flag!
docker compose up -d --build
```

to apply the changes.

:::warning
This recreates `shared-tmpfs` and all data is lost (created tables, running jobs, etc.)
:::

Make sure that the modified and added containers are up and running using

```shell
docker container ls -a
```

4. Now you are all set! You can visit
                     
- Grafana to view Fluss logs with the [log explorer](http://localhost:3002/a/grafana-lokiexplore-app/) and observe metrics of the Fluss and Flink cluster with the [provided dashboards](http://localhost:3002/dashboards) or 
- the [Prometheus Web UI](http://localhost:9092) to directly query Prometheus with [PromQL](https://prometheus.io/docs/prometheus/2.55/getting_started/).



## Cluster Monitoring (Metrics and Logs) with OpenTelemetry

This section will show you how to monitor your cluster with [OpenTelemetry](https://opentelemetry.io/).

OpenTelemetry is a vendor-neutral collection of APIs, SDKs and tools that allows to you to instrument your application to emit telemetry data.
However, OpenTelemetry does not come with an integrated observability stack. 
Instead, it lets you choose any vendor that has [support for OpenTelemetry](https://opentelemetry.io/ecosystem/vendors/).
For demonstration purposes, we will use [Prometheus](https://prometheus.io/) (metric aggregation system) and [Loki](https://grafana.com/oss/loki/) (log aggregation system).

<!-- Fix version number of Java agent -->
1. First, you need to download the [opentelemetry-javaagent](https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.17.0/opentelemetry-javaagent.jar) into your working directory.

The Java Agent offers zero-code instrumentation of telemetry data for many [popular libraries and frameworks](https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/main/docs/supported-libraries.md) without requiring any changes to the application code.
We use the Java Agent to automatically emit Log4j **logs** to the OpenTelemetry collector.
For **metrics**, we use the dedicated [Fluss OpenTelemetry metric reporter](maintenance/observability/metric-reporters.md#opentelemetry).

2. Next, you need to adapt the `docker-compose.yml` manifest and 

- configure Fluss to expose metrics via OpenTelemetry.
- set the corresponding [configuration options for the OpenTelemetry Java Agent](https://opentelemetry.io/docs/zero-code/java/agent/configuration/) and attach the agent to the Fluss application.
- add containers for the OpenTelemetry Collector, Prometheus, Loki and Grafana.
- mount the corresponding configuration files.

**Note:** OpenTelemetry can be deployed in different [modes](https://opentelemetry.io/docs/collector/deployment/). 
In this guide, we will use the [agent collector deployment pattern](https://opentelemetry.io/docs/collector/deployment/agent/).

You can simply copy the manifest below into `docker-compose.yml`.

```yaml
services:
  #begin Fluss cluster
  coordinator-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: /tmp/fluss/remote-data
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: /tmp/paimon
        metrics.reporters: opentelemetry
        metrics.reporter.opentelemetry.endpoint: http://opentelemetry-collector:4317
        metrics.reporter.opentelemetry.service.name: coordinator-server
        metrics.reporter.opentelemetry.service.version: $FLUSS_DOCKER_VERSION$
      - OTEL_SERVICE_NAME=coordinator-server
      - OTEL_SERVICE_VERSION=$FLUSS_DOCKER_VERSION$
      - OTEL_JAVAAGENT_CONFIGURATION_FILE=/etc/otel/opentelemetry-javaagent.properties
      - JAVA_TOOL_OPTIONS="-javaagent:/opt/opentelemetry-javaagent.jar"
    volumes:
      - ./fluss-quickstart-observability/opentelemetry/opentelemetry-javaagent.properties:/etc/otel/opentelemetry-javaagent.properties:ro
      - ./fluss-quickstart-observability/slf4j/log4j-opentelemetry-console.properties:/opt/fluss/conf/log4j-console.properties:ro
      - ./opentelemetry-javaagent.jar:/opt/opentelemetry-javaagent.jar:ro
  tablet-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://tablet-server:9123
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        kv.snapshot.interval: 0s
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: /tmp/paimon
        metrics.reporters: opentelemetry
        metrics.reporter.opentelemetry.endpoint: http://opentelemetry-collector:4317
        metrics.reporter.opentelemetry.service.name: tablet-server
        metrics.reporter.opentelemetry.service.version: $FLUSS_DOCKER_VERSION$
      - OTEL_SERVICE_NAME=tablet-server
      - OTEL_SERVICE_VERSION=$FLUSS_DOCKER_VERSION$
      - OTEL_JAVAAGENT_CONFIGURATION_FILE=/etc/otel/opentelemetry-javaagent.properties
      - JAVA_TOOL_OPTIONS="-javaagent:/opt/opentelemetry-javaagent.jar"
    volumes:
      - ./fluss-quickstart-observability/opentelemetry/opentelemetry-javaagent.properties:/etc/otel/opentelemetry-javaagent.properties:ro
      - ./fluss-quickstart-observability/slf4j/log4j-opentelemetry-console.properties:/opt/fluss/conf/log4j-console.properties:ro
      - ./opentelemetry-javaagent.jar:/opt/opentelemetry-javaagent.jar:ro
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  #end
  #begin Flink cluster
  jobmanager:
    image: apache/fluss-quickstart-flink:1.20-$FLUSS_DOCKER_VERSION$
    ports:
      - "8083:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - shared-tmpfs:/tmp/paimon
  taskmanager:
    image: apache/fluss-quickstart-flink:1.20-$FLUSS_DOCKER_VERSION$
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.framework.off-heap.size: 256m
    volumes:
      - shared-tmpfs:/tmp/paimon
  #end
  #begin monitoring
  opentelemetry-collector:
    image: otel/opentelemetry-collector:0.128.0
    command: "--config=/etc/otel/config.yml"
    ports:
      - "55681:55679"
    volumes:
      - ./fluss-quickstart-observability/opentelemetry/opentelemetry.yml:/etc/otel/config.yml:ro
  prometheus:
    image: bitnami/prometheus:2.55.1-debian-12-r0
    ports:
      - "9092:9090"
    depends_on:
      - opentelemetry-collector
    volumes:
      - ./fluss-quickstart-observability/prometheus/prometheus-opentelemetry.yml:/etc/prometheus/prometheus.yml:ro
  loki:
    # Do NOT use loki 2.x or older with OpenTelemetry, as this might require additional configuration!
    image: grafana/loki:3.3.2
    depends_on:
      - opentelemetry-collector
    ports:
      - "3102:3100"
  grafana:
    image:
      grafana/grafana:11.4.0
    ports:
      - "3002:3000"
    depends_on:
      - prometheus
      - loki
    volumes:
      - ./fluss-quickstart-observability/grafana:/etc/grafana:ro
  #end

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

:::warning
The `OTEL_SERVICE_NAME` and `OTEL_SERVICE_VERSION` configuration and their equivalents (e.g., in a configuration file) only apply to the agent. If you want to configure them for the Fluss OpenTelemetry Metric Reporter, you have to set them using the respective configuration options.
:::

3. Run

```shell
docker compose up -d
```

to apply the changes.

:::warning
This recreates `shared-tmpfs` and all data is lost (created tables, running jobs, etc.)
:::

Make sure that the modified and added containers are up and running using

```shell
docker container ls -a
```

and also make sure that the OpenTelemetry Collector is up and running by vising the [health endpoint](http://localhost:55681/debug/servicez). 

4. Now you are all set! You can visit
                     
- Grafana to view Fluss logs with the [log explorer](http://localhost:3002/a/grafana-lokiexplore-app/) and observe metrics of the Fluss and Flink cluster with the [provided dashboards](http://localhost:3002/dashboards) or 
- the [Prometheus Web UI](http://localhost:9092) to directly query Prometheus with [PromQL](https://prometheus.io/docs/prometheus/2.55/getting_started/).
