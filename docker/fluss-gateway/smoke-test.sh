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

IMAGE="${GATEWAY_IMAGE:-fluss-gateway:dev}"
CONTAINER="fluss-gateway-smoke-$$-${RANDOM}"

cleanup() {
    docker rm --force "${CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run \
    --detach \
    --name "${CONTAINER}" \
    --read-only \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    --publish 127.0.0.1::8080 \
    --publish 127.0.0.1::9095 \
    "${IMAGE}" >/dev/null

for _ in $(seq 1 60); do
    health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "${CONTAINER}")"
    if [[ "${health}" == "healthy" ]]; then
        break
    fi
    if [[ "$(docker inspect --format '{{.State.Running}}' "${CONTAINER}")" != "true" ]]; then
        docker logs "${CONTAINER}"
        echo "Gateway container exited before becoming healthy." >&2
        exit 1
    fi
    sleep 1
done

if [[ "$(docker inspect --format '{{.State.Health.Status}}' "${CONTAINER}")" != "healthy" ]]; then
    docker logs "${CONTAINER}"
    echo "Gateway container did not become healthy." >&2
    exit 1
fi

published_rest="$(docker port "${CONTAINER}" 8080/tcp | tail -1)"
published_metrics="$(docker port "${CONTAINER}" 9095/tcp | tail -1)"
curl --fail --silent --show-error --noproxy "*" --max-time 3 \
    "http://${published_rest}/health" >/dev/null
curl --fail --silent --show-error --noproxy "*" --max-time 3 \
    "http://${published_rest}/ready" >/dev/null
curl --fail --silent --show-error --noproxy "*" --max-time 3 \
    "http://${published_metrics}/metrics" \
    | grep -Fq "fluss_gateway_rest_requests_total"

if [[ "$(docker exec "${CONTAINER}" id -u)" != "9999" ]]; then
    echo "Gateway container must run as the fluss user (uid 9999)." >&2
    exit 1
fi
docker exec "${CONTAINER}" test -r /opt/fluss/LICENSE
docker exec "${CONTAINER}" test -r /opt/fluss/NOTICE

docker stop --time 35 "${CONTAINER}" >/dev/null
if [[ "$(docker inspect --format '{{.State.ExitCode}}' "${CONTAINER}")" != "0" ]]; then
    docker logs "${CONTAINER}"
    echo "Gateway did not exit cleanly after SIGTERM." >&2
    exit 1
fi
docker rm "${CONTAINER}" >/dev/null

if docker run \
    --name "${CONTAINER}" \
    --env FLUSS_GATEWAY__REST__LISTEN=not-an-address \
    "${IMAGE}" >/dev/null 2>&1; then
    echo "Gateway container accepted an invalid configuration." >&2
    exit 1
fi
if [[ "$(docker inspect --format '{{.State.ExitCode}}' "${CONTAINER}")" != "2" ]]; then
    docker logs "${CONTAINER}"
    echo "Gateway container must exit with code 2 for an invalid configuration." >&2
    exit 1
fi
docker rm "${CONTAINER}" >/dev/null

echo "Gateway container smoke tests passed."
