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

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
IMAGE="${GATEWAY_IMAGE:-fluss-gateway:dev}"
RELEASE_DIR="${GATEWAY_RELEASE_DIR:-${REPOSITORY_ROOT}/target/gateway-release}"
# The Dockerfile reads build-target/<arch> relative to its build context, so this
# location is fixed here and passed on explicitly rather than inherited.
BUILD_DIR="${SCRIPT_DIR}/build-target"

cleanup() {
    rm -rf "${BUILD_DIR}"
}
trap cleanup EXIT

gateway_version="$(
    sed -n '/^\[package\]/,/^\[/p' "${REPOSITORY_ROOT}/fluss-gateway/Cargo.toml" \
        | grep '^[[:space:]]*version[[:space:]]*=' \
        | head -1 \
        | sed -E 's/.*"([^"]+)".*/\1/'
)"

if [[ -z "${GATEWAY_ARCH:-}" ]]; then
    case "$(docker info --format '{{.Architecture}}')" in
        x86_64|amd64)
            GATEWAY_ARCH="amd64"
            ;;
        aarch64|arm64)
            GATEWAY_ARCH="arm64"
            ;;
        *)
            echo "Cannot determine the Docker engine architecture." >&2
            exit 1
            ;;
    esac
fi

mkdir -p "${RELEASE_DIR}"
(
    cd "${REPOSITORY_ROOT}/tools"
    RELEASE_VERSION="${gateway_version}" \
    GATEWAY_ARCH="${GATEWAY_ARCH}" \
    GATEWAY_RELEASE_DIR="${RELEASE_DIR}" \
    SKIP_GPG=true \
        releasing/create_gateway_release.sh
)

RELEASE_VERSION="${gateway_version}" \
GATEWAY_ARCHES="${GATEWAY_ARCH}" \
GATEWAY_RELEASE_DIR="${RELEASE_DIR}" \
GATEWAY_IMAGE_BUILD_DIR="${BUILD_DIR}" \
    "${SCRIPT_DIR}/prepare_build.sh"

image_version="${FLUSS_VERSION:-${gateway_version}}"
image_revision="${VCS_REF:-$(git -C "${REPOSITORY_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)}"
docker buildx build \
    --load \
    --platform "linux/${GATEWAY_ARCH}" \
    --file "${SCRIPT_DIR}/Dockerfile" \
    --build-arg "TARGETARCH=${GATEWAY_ARCH}" \
    --build-arg "FLUSS_VERSION=${image_version}" \
    --build-arg "VCS_REF=${image_revision}" \
    --tag "${IMAGE}" \
    "${SCRIPT_DIR}"

if [[ "${RUN_SMOKE:-false}" == "true" ]]; then
    GATEWAY_IMAGE="${IMAGE}" \
        "${SCRIPT_DIR}/smoke-test.sh"
fi
