# Fluss Quickstart Flink Docker

This directory contains the Docker setup for Fluss Quickstart with Flink integration.

## Overview

The Fluss Quickstart Flink Docker image provides a complete environment for running Flink with Fluss, powered by Paimon lake storage.

## Prerequisites

Before building the Docker image, ensure you have:

1. Check out the code version that you want to use for the Docker image. Go to the project root directory and build Fluss using `./mvnw clean package -DskipTests`.
The local build will be used for the Docker image.
2. Docker installed and running
3. Internet access for retrieving dependencies

## Build Process

The build process consists of two main steps:

### Step 1: Prepare Build Files

First, you need to prepare the required JAR files and dependencies:

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
docker build -t fluss/quickstart-flink:1.20-latest .
```
