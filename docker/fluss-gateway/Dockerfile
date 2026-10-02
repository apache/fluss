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

# Like the Java Fluss image, the release image only assembles a prepared
# binary distribution. Rust compilation happens before this step so buildx
# never compiles the Gateway through QEMU.
FROM debian:bookworm-slim

ARG TARGETARCH
ARG FLUSS_VERSION=dev
ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="Apache Fluss Gateway" \
      org.opencontainers.image.description="Stateless REST Gateway for Apache Fluss" \
      org.opencontainers.image.url="https://fluss.apache.org" \
      org.opencontainers.image.source="https://github.com/apache/fluss" \
      org.opencontainers.image.version="${FLUSS_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd --system --gid=9999 fluss; \
    useradd --system --home-dir /opt/fluss --create-home --uid=9999 --gid=fluss fluss

ENV FLUSS_HOME=/opt/fluss \
    FLUSS_CONF_DIR=/opt/fluss/conf \
    FLUSS_GATEWAY_CONFIG=/opt/fluss/conf/gateway.yaml \
    FLUSS_GATEWAY_HEALTH_URL=http://127.0.0.1:8080/health \
    FLUSS_GATEWAY__REST__LISTEN=0.0.0.0:8080 \
    FLUSS_GATEWAY__METRICS__EXPORTER__PROMETHEUS__LISTEN=0.0.0.0:9095 \
    HOME=/opt/fluss \
    PATH=/opt/fluss/bin:${PATH}

WORKDIR ${FLUSS_HOME}

COPY --chown=fluss:fluss build-target/${TARGETARCH}/ /opt/fluss/

USER 9999:9999

EXPOSE 8080 9095
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl --fail --silent --show-error --noproxy "*" --output /dev/null \
        --max-time 3 "${FLUSS_GATEWAY_HEALTH_URL}"

ENTRYPOINT ["/opt/fluss/bin/fluss-gateway.sh"]
