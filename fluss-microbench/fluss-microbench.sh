#!/usr/bin/env bash
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

# Fluss-Microbench launcher script.
# Usage: ./fluss-microbench.sh run --scenario-file log-append

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FLUSS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PERF_MODULE="$SCRIPT_DIR"

# Default microbench root to .microbench under the project root
MICROBENCH_ROOT="${MICROBENCH_ROOT:-$FLUSS_ROOT/.microbench}"

CP_CACHE="$PERF_MODULE/target/.classpath-cache"

# Build if needed
if [ ! -d "$PERF_MODULE/target/classes" ]; then
    echo "Building fluss-microbench..."
    mvn -f "$FLUSS_ROOT/pom.xml" install -pl fluss-microbench -am -DskipTests -Drat.skip=true -q
fi

# Generate classpath cache if missing or stale
if [ ! -f "$CP_CACHE" ] || [ "$PERF_MODULE/pom.xml" -nt "$CP_CACHE" ]; then
    mvn -f "$FLUSS_ROOT/pom.xml" dependency:build-classpath \
        -pl fluss-microbench -Drat.skip=true -q \
        -Dmdep.outputFile="$CP_CACHE"
fi

CP="$PERF_MODULE/target/classes:$(cat "$CP_CACHE")"

exec java -Dmicrobench.root="$MICROBENCH_ROOT" -cp "$CP" org.apache.fluss.microbench.PerfRunner "$@"
