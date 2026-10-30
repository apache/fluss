#!/usr/bin/env bash

#
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
#

set -o errexit
set -o nounset
set -o pipefail

# Assembles the image inputs under docker/fluss-tiering/build-target/lib:
# the Fluss tiering jars from the local Maven repository (build them first
# with ./mvnw install) and the third-party lake bundles from Maven Central.
#
# Overrides:
#   MAVEN_LOCAL_REPO            local Maven repository (default ~/.m2/repository)
#   TIERING_IMAGE_RELEASE_VERSION  Fluss version (default: read from the root pom)
#   SKIP_DOWNLOAD=true          never download; fail on jars missing locally

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
BUILD_DIR="${TIERING_IMAGE_BUILD_DIR:-${SCRIPT_DIR}/build-target}"
MAVEN_LOCAL_REPO="${MAVEN_LOCAL_REPO:-${HOME}/.m2/repository}"
MAVEN_CENTRAL="${MAVEN_CENTRAL:-https://repo1.maven.org/maven2}"

fluss_version="${TIERING_IMAGE_RELEASE_VERSION:-$(grep -A1 '<artifactId>fluss</artifactId>' "${REPOSITORY_ROOT}/pom.xml" | sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p')}"
paimon_version="$(sed -n 's/.*<paimon.version>\(.*\)<\/paimon.version>.*/\1/p' "${REPOSITORY_ROOT}/pom.xml")"
iceberg_version="$(sed -n 's/.*<iceberg.version>\(.*\)<\/iceberg.version>.*/\1/p' "${REPOSITORY_ROOT}/pom.xml")"
# Matches the flink-shaded-hadoop-2-uber version shipped with the Fluss server distribution.
hadoop_version="2.8.3-10.0"

if [[ -z "${fluss_version}" ]]; then
    echo "Could not determine the Fluss version from ${REPOSITORY_ROOT}/pom.xml." >&2
    exit 1
fi

LIB_DIR="${BUILD_DIR}/lib"
mkdir -p "${LIB_DIR}"

resolve_jar() {
    local group_path="$1" artifact="$2" version="$3" jar="$4"
    local local_jar="${MAVEN_LOCAL_REPO}/${group_path//.//}/${artifact}/${version}/${jar}"
    if [[ -s "${local_jar}" ]]; then
        cp "${local_jar}" "${LIB_DIR}/${jar}"
        echo "Copied ${jar}"
        return
    fi
    if [[ "${SKIP_DOWNLOAD:-false}" == "true" ]]; then
        echo "Missing jar (SKIP_DOWNLOAD is set): ${local_jar}" >&2
        exit 1
    fi
    local remote_url="${MAVEN_CENTRAL}/${group_path//.//}/${artifact}/${version}/${jar}"
    echo "Downloading ${remote_url}"
    curl --fail --silent --show-error --location --output "${LIB_DIR}/${jar}" "${remote_url}"
}

resolve_jar org.apache.fluss fluss-flink-tiering "${fluss_version}" "fluss-flink-tiering-${fluss_version}.jar"
resolve_jar org.apache.fluss fluss-flink-1.20 "${fluss_version}" "fluss-flink-1.20-${fluss_version}.jar"
resolve_jar org.apache.fluss fluss-lake-paimon "${fluss_version}" "fluss-lake-paimon-${fluss_version}.jar"
resolve_jar org.apache.fluss fluss-lake-iceberg "${fluss_version}" "fluss-lake-iceberg-${fluss_version}.jar"
resolve_jar org.apache.fluss fluss-lake-lance "${fluss_version}" "fluss-lake-lance-${fluss_version}.jar"

resolve_jar org.apache.paimon paimon-bundle "${paimon_version}" "paimon-bundle-${paimon_version}.jar"
resolve_jar org.apache.iceberg iceberg-flink-runtime-1.20 "${iceberg_version}" "iceberg-flink-runtime-1.20-${iceberg_version}.jar"
resolve_jar org.apache.flink flink-shaded-hadoop-2-uber "${hadoop_version}" "flink-shaded-hadoop-2-uber-${hadoop_version}.jar"

echo "Prepared tiering image inputs under ${BUILD_DIR}."
