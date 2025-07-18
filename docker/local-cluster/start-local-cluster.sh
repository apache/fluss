#!/bin/bash
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

log_stdin()
{
    echo "[`date`] $@" >&1
}

ZK_CONFIG_FILE="${FLUSS_HOME}/conf/zookeeper.properties"
echo "4lw.commands.whitelist=*" >> "$ZK_CONFIG_FILE"

log_stdin "Start Fluss Cluster"
# Start zookeeper
${FLUSS_HOME}/bin/fluss-daemon.sh start zookeeper "${FLUSS_HOME}"/conf/zookeeper.properties

# Start single Coordinator Server on this machine
${FLUSS_HOME}/bin/coordinator-server.sh start -Dbind.listeners=INTERNAL://0.0.0.0:0,CLIENT://0.0.0.0:9123 -Dadvertised.listeners=CLIENT://localhost:9123 -Dinternal.listener.name=INTERNAL

# Start single Tablet Server on this machine.
# Set bind.listeners as config option to avoid port binding conflict with coordinator server
${FLUSS_HOME}/bin/tablet-server.sh start -Dbind.listeners=INTERNAL://0.0.0.0:0,CLIENT://0.0.0.0:9124 -Dadvertised.listeners=CLIENT://localhost:9124 -Dinternal.listener.name=INTERNAL

# Health Check
# TODO：When the Fluss CLI can display service status, we will use its commands.
ZK_HOST="127.0.0.1"
ZK_PORT="2181"

NODES=("/fluss/tabletservers/ids/0" "/fluss/coordinators/active")

check_nodes_existence() {
  local zk_dump=$(echo "dump" | nc $ZK_HOST $ZK_PORT)
  local all_present=true

  for node_path in "${NODES[@]}"; do
    if ! echo "$zk_dump" | grep -q "$node_path"; then
      log_stdin "Node $node_path does not exist"
      all_present=false
    fi
  done

  $all_present
}

while sleep 30; do
  if check_nodes_existence; then
    log_stdin "Fluss service is operational."
  else
    log_stdin "Fluss service disruption detected. Exiting."
    exit 1
  fi
done
