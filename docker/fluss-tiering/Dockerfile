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

# ---------------------------------
# Lake tiering service (embedded Flink runtime)
# ---------------------------------
#
# Runs the Fluss lake tiering job on an embedded Flink runtime inside a single
# JVM process, so no separate Flink cluster needs to be provisioned. The
# official Flink distribution provides the runtime; its lib directory doubles
# as the shared classpath and carries the Fluss tiering jars plus the lake
# format bundles assembled by prepare_build.sh into build-target/lib.
#
# The image is built from this directory as the build context:
#   docker buildx build --file Dockerfile --tag fluss-tiering:dev .
#
# The Flink version of the base image must change together with the fluss-flink-1.20 module
# and the iceberg-flink-runtime-1.20 artifact in prepare_build.sh, the jar prefixes in
# smoke-test.sh, and the fluss-flink-1.20 build commands in
# fluss-flink/fluss-flink-tiering/src/README.md and
# website/docs/streaming-lakehouse/tiering-service.md.
FROM flink:1.20.3-scala_2.12-java17

ARG FLUSS_VERSION=dev
ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="Apache Fluss Tiering Service" \
      org.opencontainers.image.description="Lake tiering service for Apache Fluss with an embedded Flink runtime" \
      org.opencontainers.image.url="https://fluss.apache.org" \
      org.opencontainers.image.source="https://github.com/apache/fluss" \
      org.opencontainers.image.version="${FLUSS_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV FLUSS_HOME=/opt/fluss
ENV TIERING_CONFIG_FILE=${FLUSS_HOME}/conf/tiering.yaml

WORKDIR ${FLUSS_HOME}

# Fluss jars for the tiering service and the supported lake formats, following
# the documented tiering service layout of the Flink lib directory.
COPY build-target/lib/ /opt/flink/lib/

# Default tiering service configuration. Start-up fails with a validation
# error until fluss.bootstrap.servers and datalake.format are configured, so
# mount a configuration file at $TIERING_CONFIG_FILE or provide entries via
# the FLUSS_TIERING_PROPERTIES environment variable.
COPY tiering.yaml ${TIERING_CONFIG_FILE}

COPY docker-entrypoint.sh /
RUN ["chmod", "+x", "/docker-entrypoint.sh"]
# The base image creates the flink user; the tiering service runs as that user instead of root.
USER flink
ENTRYPOINT ["/docker-entrypoint.sh"]
# Run the tiering service by default: the base image's CMD ("help") would
# otherwise be passed to this entrypoint on a plain container run.
CMD ["tieringService"]
