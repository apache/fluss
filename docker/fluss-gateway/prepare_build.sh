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

if [[ -z "${RELEASE_VERSION:-}" ]]; then
    echo "RELEASE_VERSION was not set." >&2
    exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
RELEASE_DIR="${GATEWAY_RELEASE_DIR:-${REPOSITORY_ROOT}/tools/releasing/release}"
BUILD_DIR="${GATEWAY_IMAGE_BUILD_DIR:-${SCRIPT_DIR}/build-target}"
ARCHES="${GATEWAY_ARCHES:-amd64 arm64}"

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

for arch in ${ARCHES}; do
    case "${arch}" in
        amd64|arm64)
            ;;
        *)
            echo "Unsupported Gateway image architecture: ${arch}" >&2
            exit 1
            ;;
    esac

    package_name="fluss-gateway-${RELEASE_VERSION}-bin-linux-${arch}"
    archive="${RELEASE_DIR}/${package_name}.tgz"
    if [[ ! -s "${archive}" ]]; then
        echo "Gateway release archive is missing: ${archive}" >&2
        exit 1
    fi

    extract_dir="${BUILD_DIR}/.${arch}"
    mkdir -p "${extract_dir}"
    tar -xzf "${archive}" -C "${extract_dir}"

    package_dir="${extract_dir}/${package_name}"
    if [[ ! -d "${package_dir}" ]]; then
        echo "${archive} does not contain ${package_name}/." >&2
        exit 1
    fi
    mkdir -p "${BUILD_DIR}/${arch}"
    cp -a "${package_dir}/." "${BUILD_DIR}/${arch}/"
    rm -rf "${extract_dir}"
done

echo "Prepared Gateway image inputs under ${BUILD_DIR}."
