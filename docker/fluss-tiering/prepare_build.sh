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
#   MAVEN_LOCAL_REPO               local Maven repository (default ~/.m2/repository)
#   TIERING_IMAGE_RELEASE_VERSION  Fluss version (default: read from the root pom)
#   TIERING_IMAGE_BUILD_DIR        build directory (default <script dir>/build-target)
#   MAVEN_CENTRAL                  Maven repository for the downloads
#                                  (default https://repo1.maven.org/maven2)
#   SKIP_DOWNLOAD=true             never download; fail on jars missing locally

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
BUILD_DIR="${TIERING_IMAGE_BUILD_DIR:-${SCRIPT_DIR}/build-target}"
MAVEN_LOCAL_REPO="${MAVEN_LOCAL_REPO:-${HOME}/.m2/repository}"
MAVEN_CENTRAL="${MAVEN_CENTRAL:-https://repo1.maven.org/maven2}"

fluss_version="${TIERING_IMAGE_RELEASE_VERSION:-$(grep -A1 '<artifactId>fluss</artifactId>' "${REPOSITORY_ROOT}/pom.xml" | sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p')}"
paimon_version="$(sed -n 's/.*<paimon.version>\(.*\)<\/paimon.version>.*/\1/p' "${REPOSITORY_ROOT}/pom.xml")"
iceberg_version="$(sed -n 's/.*<iceberg.version>\(.*\)<\/iceberg.version>.*/\1/p' "${REPOSITORY_ROOT}/pom.xml")"
# Matches the flink-shaded-hadoop-2-uber version shipped with the Fluss server distribution.
hadoop_version="$(grep -A1 '<artifactId>flink-shaded-hadoop-2-uber</artifactId>' "${REPOSITORY_ROOT}/fluss-dist/pom.xml" | sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p')"

if [[ -z "${fluss_version}" ]]; then
    echo "Could not determine the Fluss version from ${REPOSITORY_ROOT}/pom.xml." >&2
    exit 1
fi
if [[ -z "${hadoop_version}" ]]; then
    echo "Could not determine the flink-shaded-hadoop-2-uber version from ${REPOSITORY_ROOT}/fluss-dist/pom.xml." >&2
    exit 1
fi

LIB_DIR="${BUILD_DIR}/lib"
mkdir -p "${LIB_DIR}"

# Computes the SHA-1 digest of a file. shasum ships with macOS and with the
# Perl toolkits of most Linux distributions; sha1sum covers the rest.
sha1_of() {
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 1 "$1" | awk '{print $1}'
    else
        sha1sum "$1" | awk '{print $1}'
    fi
}

# Verifies a downloaded jar against the .sha1 sidecar published next to it on
# the repository. A sidecar that fails to download only warns and lets the
# build continue (mirrors may not carry sidecars); a sidecar that disagrees
# with the jar aborts the build.
verify_downloaded_jar() {
    local jar_path="$1" sha1_url="$2"
    local sidecar="${jar_path}.sha1"
    if ! curl --fail --silent --show-error --location --output "${sidecar}" "${sha1_url}"; then
        echo "Warning: no checksum sidecar at ${sha1_url}; continuing without verifying ${jar_path}." >&2
        rm -f "${sidecar}"
        return 0
    fi
    local expected actual
    # the sidecar carries either a bare digest or a "digest  filename" pair
    expected="$(tr -d '\r\n' < "${sidecar}" | awk '{print $1}')"
    actual="$(sha1_of "${jar_path}")"
    if [[ "${actual}" != "${expected}" ]]; then
        echo "Checksum mismatch for ${jar_path}: expected ${expected}, computed ${actual}." >&2
        exit 1
    fi
    rm -f "${sidecar}"
    echo "Verified $(basename "${jar_path}") against its checksum sidecar."
}

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
    verify_downloaded_jar "${LIB_DIR}/${jar}" "${remote_url}.sha1"
}

# The fluss-flink-1.20 module and the iceberg-flink-runtime-1.20 artifact below track the
# Flink version of the Dockerfile base image and the fluss-flink-1.20 build commands in
# fluss-flink/fluss-flink-tiering/src/README.md and
# website/docs/streaming-lakehouse/tiering-service.md; change them all together.
# When changing the bundled jars, update expected_jar_prefixes in smoke-test.sh.
resolve_jar org.apache.fluss fluss-flink-tiering "${fluss_version}" "fluss-flink-tiering-${fluss_version}.jar"
resolve_jar org.apache.fluss fluss-flink-1.20 "${fluss_version}" "fluss-flink-1.20-${fluss_version}.jar"
resolve_jar org.apache.fluss fluss-lake-paimon "${fluss_version}" "fluss-lake-paimon-${fluss_version}.jar"
resolve_jar org.apache.fluss fluss-lake-iceberg "${fluss_version}" "fluss-lake-iceberg-${fluss_version}.jar"
resolve_jar org.apache.fluss fluss-lake-lance "${fluss_version}" "fluss-lake-lance-${fluss_version}.jar"

resolve_jar org.apache.paimon paimon-bundle "${paimon_version}" "paimon-bundle-${paimon_version}.jar"
resolve_jar org.apache.iceberg iceberg-flink-runtime-1.20 "${iceberg_version}" "iceberg-flink-runtime-1.20-${iceberg_version}.jar"
resolve_jar org.apache.flink flink-shaded-hadoop-2-uber "${hadoop_version}" "flink-shaded-hadoop-2-uber-${hadoop_version}.jar"

echo "Prepared tiering image inputs under ${BUILD_DIR}."
