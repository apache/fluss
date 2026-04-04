#!/bin/bash
#
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
#

# Builds a local Docker image from the current source tree.
#
# Usage:
#   ./build-local-image.sh              # build with default image name
#   ./build-local-image.sh my-image:tag # build with custom image name

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
IMAGE_NAME="${1:-fluss-local:latest}"
DOCKER_DIR="$PROJECT_ROOT/docker/fluss"

echo "=== Step 1: Building Fluss distribution with Maven ==="
cd "$PROJECT_ROOT"
mvn package -pl fluss-dist -am \
    -DskipTests \
    -Dcheckstyle.skip=true \
    -Dspotless.check.skip=true

if [ ! -d "$PROJECT_ROOT/build-target" ]; then
    echo "ERROR: build-target directory not found after Maven build."
    echo "       Expected symlink at: $PROJECT_ROOT/build-target"
    exit 1
fi

echo ""
echo "=== Step 2: Copying build artifacts to Docker context ==="
rm -rf "$DOCKER_DIR/build-target"
cp -rL "$PROJECT_ROOT/build-target/" "$DOCKER_DIR/build-target/"
echo "  Copied build-target/ to $DOCKER_DIR/build-target/"

echo ""
echo "=== Step 3: Building Docker image ==="
docker build -t "$IMAGE_NAME" "$DOCKER_DIR"

echo ""
echo "=== Step 4: Cleaning up ==="
rm -rf "$DOCKER_DIR/build-target"
echo "  Removed $DOCKER_DIR/build-target/"

echo ""
echo "========================================="
echo "  Image built: $IMAGE_NAME"
echo "  Launch with: docker compose up -d"
echo "========================================="
