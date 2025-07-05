# Fluss Examples

This directory contains example code for using Fluss.

# Prerequisites

- Docker + Compose installed on your machine.

## Running the Environment

Execute the docker compose to spin up a Fluss cluster, but create the Fluss network first:

```bash
docker network create fluss-net
docker compose up
```

## Building examples

You can build the examples using Maven. Navigate to the `fluss-examples` directory and run:

```bash
mvn clean package
```

## Runing the Examples

You maybe will need setup arrow environment variables to run the examples:

```bash
JDK_JAVA_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED" java 






