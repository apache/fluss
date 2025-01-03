---
sidebar_label: Quickstart Guides
sidebar_position: 1
---

# Observability Quickstart Guides

On this page, you can find the following guides to set up an observability stack **based on the instructions in the [Flink quickstart guide](/docs/quickstart/flink)**:

- [Observability with Prometheus, Loki and Grafana](#observability-with-prometheus-loki-and-grafana)

:::warning
    Make sure the environment variables for the Fluss and the Quickstart version are set.
    For further information, check the [Flink quickstart guide](/docs/quickstart/flink#starting-required-components).
:::

## Observability with Prometheus, Loki and Grafana

We provide a minimal quickstart configuration for application observability with Prometheus (metric aggregation system), Loki (log aggregation sytem) and Grafana (dashboard system). 

The quickstart configuration comes with 2 metric dashboards.

- `Fluss – overview`: Selected metrics to observe the overall cluster status
- `Fluss – detail`: Majority of metrics listed in [metrics list](./monitor-metrics.md#metrics-list)

Follow the instructions below to add observability capabilities to your setup.

1. Download the <a href={ require("../../assets/fluss-quickstart-observability.zip").default } target="_blank">observability quickstart configuration</a> and extract the ZIP archive in your working directory.
After extracting the archive, the contents of the working directory should be as follows.

```
├── docker-compose.yml              # docker compose manifest from quickstart guide
└── fluss-quickstart-observability  # downloaded and extracted ZIP archive
    ├── grafana
    │   ├── grafana.ini
    │   └── provisioning
    │       ├── dashboards
    │       │   ├── default.yml
    │       │   └── fluss
    │       │       └── ...
    │       └── datatsources
    │           └── default.yml
    ├── prometheus
    │   └── prometheus.yml
    └── slf4j
        └── ...
```

2. Next, you need to configure Fluss to expose logs to Loki. We will use [Loki4j](https://loki4j.github.io/loki-logback-appender/) which uses Logback as logging backend.
The container manifest below configures Fluss to use Logback and Loki4j. Save it to a file named `fluss-slf4j-logback.Dockerfile` in your working directory.

```dockerfile
FROM fluss/fluss:${FLUSS_VERSION}

# remove default logging backend from classpath and add logback to classpath
RUN rm -rf ${FLUSS_HOME}/lib/log4j-slf4j-impl-*.jar && \
    wget https://repo1.maven.org/maven2/ch/qos/logback/logback-classic/1.2.13/logback-classic-1.2.13.jar -P ${FLUSS_HOME}/lib/ && \
    wget https://repo1.maven.org/maven2/ch/qos/logback/logback-core/1.2.13/logback-core-1.2.13.jar -P ${FLUSS_HOME}/lib/

# add loki4j logback appender to classpath
RUN wget https://repo1.maven.org/maven2/com/github/loki4j/loki-logback-appender/1.4.2/loki-logback-appender-1.4.2.jar -P ${FLUSS_HOME}/lib/

# logback configuration that exposes metrics to loki
COPY fluss-quickstart-observability/slf4j/logback-loki-console.xml ${FLUSS_HOME}/conf/logback-console.xml
```

:::note
Detailed configuration instructions for Fluss and Logback can be found [here](./logging.md#configuring-logback).
:::

3. Additionally, you need to adapt the `docker-compose.yml` and 

- add containers for Prometheus, Loki and Grafana and mount the corresponding configuration directories.
- build and use the new Fluss image manifest (`fluss-sfl4j-logback.Dockerfile`).
- configure Fluss to expose metrics via Prometheus.
- add the desired application name that should be used when displaying logs in Grafana as environment variable (`APP_NAME`).
- configure Flink to expose metrics via Prometheus.

To do this, you can simply copy the manifest below into your `docker-compose.yml`.

```yaml
services:
  #begin Fluss cluster
  coordinator-server:
    image: fluss-slf4j-logback:${FLUSS_VERSION}
    build:
      dockerfile: fluss-slf4j-logback.Dockerfile
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        coordinator.host: coordinator-server
        remote.data.dir: /tmp/fluss/remote-data
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: /tmp/paimon
        metrics.reporters: prometheus
        metrics.reporter.prometheus.port: 9250
        logback.configurationFile: logback-loki-console.xml
      - APP_NAME=coordinator-server
  tablet-server:
    image: fluss-slf4j-logback:${FLUSS_VERSION}
    build:
      dockerfile: fluss-slf4j-logback.Dockerfile
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        kv.snapshot.interval: 0s
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: /tmp/paimon
        metrics.reporters: prometheus
        metrics.reporter.prometheus.port: 9250
        logback.configurationFile: logback-loki-console.xml
      - APP_NAME=tablet-server
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  #end
  #begin Flink cluster
  jobmanager:
    image: fluss/quickstart-flink:${FLUSS_QUICKSTART_FLINK_VERSION}
    ports:
      - "8083:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
        metrics.reporter.prom.port: 9250
    volumes:
      - shared-tmpfs:/tmp/paimon
  taskmanager:
    image: fluss/quickstart-flink:${FLUSS_QUICKSTART_FLINK_VERSION}
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
        metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
        metrics.reporter.prom.port: 9250
    volumes:
      - shared-tmpfs:/tmp/paimon
  #end
  #begin observability
  prometheus:
    image: bitnami/prometheus:2.55.1-debian-12-r0
    ports:
      - "9092:9090"
    volumes:
      - ./fluss-quickstart-observability/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
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

Then run

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
docker ps
```

4. Now you are all set! You can visit
                     
- Grafana to view Fluss logs with the [log explorer](http://localhost:3002/a/grafana-lokiexplore-app/) and observe metrics of the Fluss and Flink cluster with the [provided dashboards](http://localhost:3002/dashboards) or 
- the [Prometheus Web UI](http://localhost:9092) to directly query Prometheus with [PromQL](https://prometheus.io/docs/prometheus/2.55/getting_started/).