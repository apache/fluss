---
sidebar_position: 1
title: Building Fluss
---

# Building Fluss from Source

This page covers how to build Fluss from sources.

In order to build Fluss you need to get the source code by [cloning the git repository](https://github.com/apache/fluss).

In addition, you need **Maven 3.8.6** and a **JDK** (Java Development Kit). Fluss requires **Java 11** to build.

To clone from git, enter:

```bash
git clone git@github.com:apache/fluss.git
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