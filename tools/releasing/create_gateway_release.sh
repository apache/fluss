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

SKIP_GPG=${SKIP_GPG:-false}

if [[ -z "${RELEASE_VERSION:-}" ]]; then
    echo "RELEASE_VERSION was not set." >&2
    exit 1
fi

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

CURR_DIR="$(pwd)"
if [[ "$(basename "${CURR_DIR}")" != "tools" ]]; then
    echo "You have to call the script from the tools/ dir" >&2
    exit 1
fi

if [[ "$(uname)" == "Darwin" ]]; then
    SHASUM=(shasum -a 512)
    TAR_OPTIONS=(--no-xattrs)
    export COPYFILE_DISABLE=1
else
    SHASUM=(sha512sum)
    TAR_OPTIONS=()
fi

FLUSS_DIR="$(cd .. && pwd -P)"
GATEWAY_DIR="${FLUSS_DIR}/fluss-gateway"
RELEASE_DIR="${GATEWAY_RELEASE_DIR:-${FLUSS_DIR}/tools/releasing/release}"
mkdir -p "${RELEASE_DIR}"

gateway_version="$(
    sed -n '/^\[package\]/,/^\[/p' "${GATEWAY_DIR}/Cargo.toml" \
        | grep '^[[:space:]]*version[[:space:]]*=' \
        | head -1 \
        | sed -E 's/.*"([^"]+)".*/\1/'
)"
if [[ "${gateway_version}" != "${RELEASE_VERSION}" ]]; then
    echo "Gateway version ${gateway_version} does not match RELEASE_VERSION ${RELEASE_VERSION}." >&2
    exit 1
fi

if [[ -z "${GATEWAY_ARCH:-}" ]]; then
    case "$(uname -m)" in
        x86_64)
            GATEWAY_ARCH="amd64"
            ;;
        aarch64|arm64)
            GATEWAY_ARCH="arm64"
            ;;
        *)
            echo "Set GATEWAY_ARCH to amd64 or arm64." >&2
            exit 1
            ;;
    esac
fi

case "${GATEWAY_ARCH}" in
    amd64|arm64)
        ;;
    *)
        echo "Unsupported Gateway release architecture: ${GATEWAY_ARCH}" >&2
        exit 1
        ;;
esac

temporary_dir="$(mktemp -d "${RELEASE_DIR}/.gateway-release.XXXXXX")"
cleanup() {
    rm -rf "${temporary_dir}"
}
trap cleanup EXIT

if ! docker buildx version >/dev/null 2>&1; then
    echo "Docker with buildx is required to build the Gateway release binary." >&2
    exit 1
fi

# The release manager may run this on macOS, whose Bash 3.2 treats "${array[@]}"
# as an unbound variable when the array is empty. Expand optional arguments with
# the ${array[@]+...} guard so an empty array stays empty instead of aborting.
build_cache_args=()
if [[ -n "${GATEWAY_BUILD_CACHE_FROM:-}" ]]; then
    build_cache_args+=(--cache-from "${GATEWAY_BUILD_CACHE_FROM}")
fi
if [[ -n "${GATEWAY_BUILD_CACHE_TO:-}" ]]; then
    build_cache_args+=(--cache-to "${GATEWAY_BUILD_CACHE_TO}")
fi
docker buildx build \
    --platform "linux/${GATEWAY_ARCH}" \
    --file "${FLUSS_DIR}/docker/fluss-gateway/Dockerfile.build" \
    --target artifact \
    ${build_cache_args[@]+"${build_cache_args[@]}"} \
    --output "type=local,dest=${temporary_dir}/binary" \
    "${FLUSS_DIR}"
gateway_binary="${temporary_dir}/binary/fluss-gateway"

if [[ ! -x "${gateway_binary}" ]]; then
    echo "Gateway binary is missing or not executable: ${gateway_binary}" >&2
    exit 1
fi

case "$(uname -s)/$(uname -m)" in
    Linux/x86_64)
        native_arch="amd64"
        ;;
    Linux/aarch64|Linux/arm64)
        native_arch="arm64"
        ;;
    *)
        native_arch=""
        ;;
esac
if [[ "${GATEWAY_ARCH}" == "${native_arch}" ]]; then
    binary_version="$("${gateway_binary}" --version)"
    if [[ "${binary_version}" != "fluss-gateway ${RELEASE_VERSION}" ]]; then
        echo "Gateway binary version does not match RELEASE_VERSION: ${binary_version}" >&2
        exit 1
    fi
fi

package_name="fluss-gateway-${RELEASE_VERSION}-bin-linux-${GATEWAY_ARCH}"
package_dir="${temporary_dir}/${package_name}"
archive_name="${package_name}.tgz"
archive="${RELEASE_DIR}/${archive_name}"

rm -f "${archive}" "${archive}.asc" "${archive}.sha512"
mkdir -p "${package_dir}/bin" "${package_dir}/conf"

install -m 0755 "${gateway_binary}" "${package_dir}/bin/fluss-gateway"
install -m 0755 "${GATEWAY_DIR}/bin/fluss-gateway.sh" "${package_dir}/bin/"
install -m 0644 "${GATEWAY_DIR}/conf/gateway.yaml" "${package_dir}/conf/"
install -m 0644 "${GATEWAY_DIR}/openapi.yaml" "${package_dir}/"
install -m 0644 "${GATEWAY_DIR}/DEPENDENCIES.rust.tsv" "${package_dir}/"
install -m 0644 "${GATEWAY_DIR}/LICENSE-bin" "${package_dir}/LICENSE"
install -m 0644 "${GATEWAY_DIR}/NOTICE-bin" "${package_dir}/NOTICE"

(
    cd "${temporary_dir}"
    tar ${TAR_OPTIONS[@]+"${TAR_OPTIONS[@]}"} -czf "${archive}" "${package_name}"
)

(
    cd "${RELEASE_DIR}"
    if [[ "${SKIP_GPG}" == "false" ]]; then
        gpg --armor --detach-sig "${archive_name}"
    fi
    "${SHASUM[@]}" "${archive_name}" > "${archive_name}.sha512"
)

echo "Created ${archive}."
