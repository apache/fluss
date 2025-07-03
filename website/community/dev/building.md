---
sidebar_position: 1
title: Building Fluss
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

# Building Fluss from Source

This page covers how to build Fluss from sources.

In order to build Fluss you need to get the source code by [cloning the git repository](https://github.com/alibaba/fluss).

In addition, you need **Maven 3.8.6** and a **JDK** (Java Development Kit). Fluss requires **Java 8 or Java 11** to build.

:::tip
- It is recommended to use JDK 1.8.3 if you're using Java 8 and encounter an error like `java.lang.NoSuchMethodError: 'java.nio.MappedByteBuffer java.nio.MappedByteBuffer.duplicate()'`.
- Setting the environment variable via `export JAVA_TOOL_OPTIONS="-XX:+UnlockExperimentalVMOptions"` is useful if you encounter an error `VM option 'UseG1GC' is experimental and must be enabled via -XX:+UnlockExperimentalVMOptions`.
:::

To clone from git, enter:

```bash
git clone git@github.com:alibaba/fluss.git
```

If you want to build a specific release or release candidate, have a look at the existing tags using 

```bash
git tag -n
```

and checkout the corresponding branch using

```bash
git checkout <tag>
```

The simplest way of building Fluss is by running:

```bash
mvn clean install -DskipTests
```

This instructs [Maven](http://maven.apache.org) (`mvn`) to first remove all existing builds (`clean`) and then create a new Fluss binary (`install`).

:::tip
Using the included [Maven Wrapper](https://maven.apache.org/wrapper/) by replacing `mvn` with `./mvnw` ensures that the correct Maven version is used.
:::

To speed up the build you can:
- skip tests by using ` -DskipTests`
- use Maven's parallel build feature, e.g., `mvn package -T 1C` will attempt to build 1 module for each CPU core in parallel.

The build script will be:
```bash
mvn clean install -DskipTests -T 1C
```

**NOTE**:
- For local testing, it's recommend to use directory `${project}/build-target` in project.
- For deploying distributed cluster, it's recommend to use binary file named `fluss-xxx-bin.tgz`, the file is in directory `${project}/fluss-dist/target`.