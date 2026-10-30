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

# Smoke tests for the tiering service image. The embedded tiering job needs a
# reachable Fluss cluster, so these checks cover the wiring around it instead:
# the bundled jars, the entrypoint modes, and the fast-fail on missing
# configuration.

IMAGE="${TIERING_IMAGE:-fluss-tiering:dev}"
CONTAINER="fluss-tiering-smoke-$$-${RANDOM}"

cleanup() {
    docker rm --force "${CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! docker run --rm "${IMAGE}" bash -c \
    'ls /opt/flink/lib | grep -q "fluss-flink-tiering-"'; then
    echo "The image does not bundle the Fluss tiering jar." >&2
    exit 1
fi

help_output="$(docker run --rm "${IMAGE}" help)"
if ! grep -q "tieringService" <<< "${help_output}"; then
    echo "The entrypoint help does not describe the tieringService mode." >&2
    exit 1
fi

# FLUSS_TIERING_PROPERTIES entries and ${VAR} references must be substituted
# by the entrypoint (envsubst): with the bootstrap servers resolved from the
# environment, the service fails on the next missing setting instead of the
# bootstrap servers, so a missing or broken envsubst fails this check.
substitution_run_output="$(docker run --name "${CONTAINER}" \
    -e BOOTSTRAP_HOST="smoke-bootstrap:9123" \
    -e 'FLUSS_TIERING_PROPERTIES=fluss.bootstrap.servers: ${BOOTSTRAP_HOST}' \
    "${IMAGE}" 2>&1 || true)"
if ! grep -q "datalake.format is not configured" <<< "${substitution_run_output}"; then
    echo "The entrypoint did not substitute the FLUSS_TIERING_PROPERTIES ${VAR} reference." >&2
    echo "${substitution_run_output}"
    exit 1
fi
docker rm "${CONTAINER}" >/dev/null

# Without configuration the service must fail fast with the validation error,
# exiting non-zero so a container runtime can restart it once it is configured.
run_output="$(docker run --name "${CONTAINER}" "${IMAGE}" 2>&1 || true)"
if ! grep -q "bootstrap server to fluss is not configured" <<< "${run_output}"; then
    echo "The service did not fail on the missing Fluss bootstrap servers." >&2
    echo "${run_output}"
    exit 1
fi
if [[ "$(docker inspect --format '{{.State.ExitCode}}' "${CONTAINER}")" == "0" ]]; then
    echo "The service must exit non-zero on missing configuration." >&2
    exit 1
fi
docker rm "${CONTAINER}" >/dev/null

echo "Tiering service container smoke tests passed."
