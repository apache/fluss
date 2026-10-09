#!/usr/bin/env bash

################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -euo pipefail

DEVKIT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(dirname "$DEVKIT_DIR")

paimon_version=$(sed -n 's:.*<paimon.version>\([^<]*\)</paimon.version>.*:\1:p' "$ROOT_DIR/pom.xml")
iceberg_version=$(sed -n 's:.*<iceberg.version>\([^<]*\)</iceberg.version>.*:\1:p' "$ROOT_DIR/pom.xml")

grep -Fq "/paimon-s3/${paimon_version}/paimon-s3-${paimon_version}.jar" "$DEVKIT_DIR/profiles/paimon/jars.urls"
grep -Fq "/paimon-flink-1.20/${paimon_version}/paimon-flink-1.20-${paimon_version}.jar" "$DEVKIT_DIR/profiles/paimon/flink.urls"
grep -Fq "/iceberg-aws/${iceberg_version}/iceberg-aws-${iceberg_version}.jar" "$DEVKIT_DIR/profiles/iceberg/jars.urls"
grep -Fq "/iceberg-aws-bundle/${iceberg_version}/iceberg-aws-bundle-${iceberg_version}.jar" "$DEVKIT_DIR/profiles/iceberg/jars.urls"
grep -Fq "/iceberg-flink-runtime-1.20/${iceberg_version}/iceberg-flink-runtime-1.20-${iceberg_version}.jar" "$DEVKIT_DIR/profiles/iceberg/flink.urls"
