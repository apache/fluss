<!--
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->

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






