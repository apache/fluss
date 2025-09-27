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

# Use Flink 1.20.0 as base image
FROM flink:1.20.0-scala_2.12-java17

# Switch to root user for installation and setup
USER root

# Install necessary packages
RUN apt-get update && \
    apt-get install -y tree && \
    rm -rf /var/lib/apt/lists/*

# Copy sql-client script to the container
COPY bin/* /opt/sql-client/

# Set working directory and environment
WORKDIR /opt/sql-client
ENV SQL_CLIENT_HOME=/opt/sql-client

# Copy Fluss connector JARs and SQL files
# Copy JARs to both sql-client lib and Flink lib directories
COPY lib/* /opt/sql-client/lib/
COPY sql/* /opt/sql-client/sql/
COPY lib/* /opt/flink/lib/
COPY opt/* /opt/flink/opt/

# Modify docker-entrypoint.sh to allow Flink to run as root user
# This is needed for the quickstart environment
RUN sed -i 's/exec $(drop_privs_cmd)/exec/g' /docker-entrypoint.sh

# Make sql-client script executable
RUN ["chmod", "+x", "/opt/sql-client/sql-client"]
