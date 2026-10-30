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

# Builds the tiering service image: prepare_build.sh assembles the image
# inputs, docker buildx builds the image, and the smoke tests run on demand.
#
# Overrides:
#   TIERING_IMAGE    image name:tag (default fluss-tiering:dev)
#   FLUSS_VERSION    Fluss version for the image label and the jars
#                    prepare_build.sh selects (default: read from the root pom)
#   VCS_REF          source revision for the image label (default: git HEAD)
#   RUN_SMOKE=true   run the smoke tests against the built image

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
IMAGE="${TIERING_IMAGE:-fluss-tiering:dev}"

fluss_version="$(grep -A1 '<artifactId>fluss</artifactId>' "${REPOSITORY_ROOT}/pom.xml" | sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p')"
image_version="${FLUSS_VERSION:-${fluss_version}}"
image_revision="${VCS_REF:-$(git -C "${REPOSITORY_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)}"

TIERING_IMAGE_RELEASE_VERSION="${image_version}" "${SCRIPT_DIR}/prepare_build.sh"

docker buildx build \
    --load \
    --file "${SCRIPT_DIR}/Dockerfile" \
    --build-arg "FLUSS_VERSION=${image_version}" \
    --build-arg "VCS_REF=${image_revision}" \
    --tag "${IMAGE}" \
    "${SCRIPT_DIR}"

if [[ "${RUN_SMOKE:-false}" == "true" ]]; then
    TIERING_IMAGE="${IMAGE}" \
        "${SCRIPT_DIR}/smoke-test.sh"
fi
