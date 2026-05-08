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

# Fluss Quickstart Flink Docker

This directory contains the Docker setup for the `apache/fluss-quickstart-flink` image used by the Fluss quickstart guides.

## Overview

The image now separates dependencies into three clearly defined groups:

- `lib/`: base Fluss quickstart dependencies copied directly into `FLINK_HOME/lib`
- `paimon/`: optional Paimon lakehouse dependencies activated by `/opt/flink/init_paimon.sh`
- `iceberg/`: optional Iceberg lakehouse dependencies activated by `/opt/flink/init_iceberg.sh`

This split keeps the regular "Quickstart with Flink" flow working out of the box while still allowing the Paimon and Iceberg lakehouse guides to opt in to their extra jars explicitly.

The optional lakehouse directories include the Flink-side filesystem and catalog jars needed by those guides. For example:

- `paimon/` includes `paimon-flink-1.20-1.3.1`, `paimon-s3-1.3.1`, and `hadoop-apache`
- `iceberg/` includes `iceberg-flink-runtime`, `hadoop-apache`, `iceberg-aws`, `iceberg-aws-bundle`, and `postgresql`

## Important Behavior

- The standard Flink quickstart should start with no custom entrypoint and no manual jar copying.
- Paimon quickstart containers must use `/opt/flink/init_paimon.sh` before starting Flink.
- Iceberg quickstart containers must use `/opt/flink/init_iceberg.sh` before starting Flink.
- The `/opt/sql-client/sql-client` helper is only for the "Real-Time Analytics with Flink" quickstart and preloads demo SQL objects from `sql/sql-client.sql`.

## Prerequisites

Before building the Docker image, ensure you have:

1. Check out the code version that you want to use for the Docker image. Go to the project root directory and build Fluss using `./mvnw clean package -DskipTests`.
The local build artifacts will be used for the Docker image.
2. Docker installed and running
3. Internet access for retrieving dependencies

## Build Process

The build process consists of two main steps:

### Step 1: Prepare Build Files

First, prepare the required base and optional dependency directories:

```bash
# Make the script executable
chmod +x prepare_build.sh

# Run the preparation script
./prepare_build.sh
```

### Step 2: Build Docker Image

After the preparation is complete, build the Docker image:

```bash
# Build the Docker image
docker build -t apache/fluss-quickstart-flink .
```

After `prepare_build.sh` finishes, verify that the following directories exist:

- `lib/`
- `paimon/`
- `iceberg/`
- `opt/`

If you are wiring the image into Docker Compose:

- use `command: ["/opt/sql-client/sql-client"]` for SQL clients that should preload the demo source tables
- use `entrypoint: ["/opt/flink/init_paimon.sh"]` for Paimon services
- use `entrypoint: ["/opt/flink/init_iceberg.sh"]` for Iceberg services
