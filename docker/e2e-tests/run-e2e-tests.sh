#!/bin/bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# E2E Test Runner for Fluss Iceberg V3 Integration
# Usage: ./run-e2e-tests.sh [--skip-build] [--keep-cluster]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SKIP_BUILD=false
KEEP_CLUSTER=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --keep-cluster)
            KEEP_CLUSTER=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "Fluss Iceberg V3 E2E Test Runner"
echo "=========================================="

# Step 1: Build Fluss if needed
if [ "$SKIP_BUILD" = false ]; then
    echo ""
    echo "[Step 1/5] Building Fluss..."
    cd "${PROJECT_ROOT}"
    mvn -T 1C -B clean package -DskipTests -Pdist -q
    echo "Build complete."
else
    echo ""
    echo "[Step 1/5] Skipping build (--skip-build)"
fi

# Step 2: Prepare Docker image
echo ""
echo "[Step 2/5] Preparing Fluss Docker image..."
cd "${PROJECT_ROOT}"
rm -rf docker/fluss/build-target
mkdir -p docker/fluss/build-target
tar -xzf fluss-dist/target/fluss-*.tgz -C docker/fluss/build-target --strip-components=1

cd docker/fluss
docker build -t fluss/fluss:e2e-test . -q
echo "Docker image built: fluss/fluss:e2e-test"

# Step 3: Start cluster
echo ""
echo "[Step 3/5] Starting Fluss cluster..."
cd "${SCRIPT_DIR}"

# Stop any existing cluster
docker-compose down -v 2>/dev/null || true

# Start fresh cluster
export FLUSS_IMAGE=fluss/fluss:e2e-test
docker-compose up -d

echo "Waiting for cluster to be ready..."
sleep 30

# Check cluster health
echo "Checking cluster health..."
docker-compose ps

# Verify ZooKeeper
echo "Checking ZooKeeper..."
docker exec fluss-zookeeper zkServer.sh status || true

# Verify MinIO
echo "Checking MinIO..."
curl -sf http://localhost:9000/minio/health/live && echo "MinIO is healthy" || echo "MinIO health check pending"

# Wait more for Fluss services
echo "Waiting for Fluss services..."
sleep 30

# Step 4: Run E2E tests
echo ""
echo "[Step 4/5] Running E2E tests..."
cd "${PROJECT_ROOT}"

export FLUSS_BOOTSTRAP_SERVERS=localhost:9123
export ICEBERG_WAREHOUSE=s3://iceberg-warehouse/
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT=http://localhost:9000

mvn -B test \
    -pl fluss-lake/fluss-lake-iceberg \
    -Dtest=IcebergV3E2ETest \
    -DfailIfNoTests=false

TEST_RESULT=$?

# Step 5: Cleanup
echo ""
echo "[Step 5/5] Cleanup..."
if [ "$KEEP_CLUSTER" = false ]; then
    cd "${SCRIPT_DIR}"
    docker-compose down -v
    echo "Cluster stopped."
else
    echo "Cluster kept running (--keep-cluster)"
    echo "To stop: cd ${SCRIPT_DIR} && docker-compose down -v"
fi

# Report result
echo ""
echo "=========================================="
if [ $TEST_RESULT -eq 0 ]; then
    echo "E2E Tests: PASSED"
else
    echo "E2E Tests: FAILED"
fi
echo "=========================================="

exit $TEST_RESULT
