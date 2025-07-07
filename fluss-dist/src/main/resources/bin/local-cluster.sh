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


USAGE="Usage: $0 start|stop [--zk-opts \"opts\"] [--coordinator-opts \"opts\"] [--tablet-opts \"opts\"]"

STARTSTOP=$1

if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "stop" ]]; then
  echo $USAGE
  exit 1
fi

# Define default configurations
ZK_OPTS="${ZK_OPTS:-}"
COORDINATOR_OPTS="${COORDINATOR_OPTS:-}"
TABLET_OPTS="${TABLET_OPTS:--Dbind.listeners=FLUSS://localhost:0}"

# Process command line arguments
shift  # Remove the first argument (start/stop)
while [[ $# -gt 0 ]]; do
    case $1 in
        --zk-opts)
            ZK_OPTS="$2"
            shift 2
            ;;
        --coordinator-opts)
            COORDINATOR_OPTS="$2"
            shift 2
            ;;
        --tablet-opts)
            TABLET_OPTS="$2"
            shift 2
            ;;
        *)
            echo "Unknown parameter: $1"
            echo "$USAGE"
            exit 1
            ;;
    esac
done

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

case $STARTSTOP in
    (start)
        echo "Starting cluster."

        # Start zookeeper
        "$FLUSS_BIN_DIR"/fluss-daemon.sh start zookeeper "${FLUSS_CONF_DIR}"/zookeeper.properties $ZK_OPTS

        # Start single Coordinator Server on this machine
        "$FLUSS_BIN_DIR"/coordinator-server.sh start $COORDINATOR_OPTS

        # Start single Tablet Server on this machine.
        # Set bind.listeners as config option to avoid port binding conflict with coordinator server
        "${FLUSS_BIN_DIR}"/tablet-server.sh start $TABLET_OPTS
    ;;

    (stop)
        echo "Stopping cluster."

        "$FLUSS_BIN_DIR"/tablet-server.sh stop

        "$FLUSS_BIN_DIR"/coordinator-server.sh stop

        "$FLUSS_BIN_DIR"/fluss-daemon.sh stop zookeeper
    ;;

    (*)
        echo $USAGE
        exit 1
    ;;

esac

