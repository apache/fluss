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

# Base image for all Fluss quickstart scenarios.
FROM flink:1.20.0-scala_2.12-java17

# The init scripts copy optional lakehouse jars into /opt/flink/lib at runtime,
# so keep the quickstart container running as root like the previous image.
USER root

# Put the SQL helper into a dedicated directory. The helper is used by the
# "Real-Time Analytics with Flink" quickstart to pre-load demo source tables.
COPY bin/ /opt/sql-client/
COPY sql/ /opt/sql-client/sql/

# Keep the base Fluss quickstart dependencies in /opt/flink/lib so the standard
# Flink quickstart works out of the box with no extra container setup.
COPY lib/ /opt/flink/lib/

# Keep optional lakehouse dependencies out of /opt/flink/lib by default.
# The init_paimon.sh / init_iceberg.sh scripts activate them explicitly when a
# Paimon or Iceberg quickstart container starts.
COPY paimon/ /opt/flink/paimon/
COPY iceberg/ /opt/flink/iceberg/
COPY opt/ /opt/flink/opt/
COPY bin/init_paimon.sh /opt/flink/init_paimon.sh
COPY bin/init_iceberg.sh /opt/flink/init_iceberg.sh

WORKDIR /opt/sql-client
ENV SQL_CLIENT_HOME=/opt/sql-client

RUN chmod +x \
    /opt/sql-client/sql-client \
    /opt/flink/init_paimon.sh \
    /opt/flink/init_iceberg.sh
