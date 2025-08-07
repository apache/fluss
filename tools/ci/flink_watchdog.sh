#!/usr/bin/env bash
################################################################################
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
################################################################################

# Simple watchdog for Flink tests in Fluss CI
# Captures thread dumps when tests are approaching timeout

COMMAND=$@

if [ -n "${GITHUB_ACTIONS+x}" ]; then
  echo "[INFO] GitHub Actions environment detected"
  job_timeout=${FLINK_TEST_TIMEOUT:-60}
  temporary_folder="${RUNNER_TEMP}"
else
  echo "[ERROR] Only GitHub Actions environment is supported"
  exit 1
fi

# Setup debug directory
debug_dir="${temporary_folder}/flink-debug"
mkdir -p $debug_dir || { echo "FAILURE: cannot create debug directory" ; exit 1; }

# Install moreutils for timestamping
sudo apt-get install -y moreutils

# Time calculations
REAL_START_SECONDS=$(date +"%s")
REAL_TIMEOUT_SECONDS=$((job_timeout * 60))
KILL_SECONDS_BEFORE_TIMEOUT=$((2 * 60))

echo "Running Flink tests with timeout of $job_timeout minutes"

MAIN_PID_FILE="/tmp/flink_watchdog_main.pid"

function print_stacktraces() {
  echo "=== Java processes ==="
  jps -v || echo "jps not available"
  echo ""

  echo "=== Thread dumps ==="
  for pid in $(jps -q); do
    if [ -n "$pid" ]; then
      echo "Stack trace for PID $pid:"
      jstack $pid || echo "Could not get stack trace for $pid"
      echo ""
    fi
  done
}

function timeout_watchdog() {
  # 95%
  sleep $(($REAL_TIMEOUT_SECONDS * 95 / 100))
  echo "=========================================================================================="
  echo "=== WARNING: Flink tests took 95% of available time budget of $job_timeout minutes ==="
  echo "=========================================================================================="
  print_stacktraces | tee "$debug_dir/jps-traces.0"

  # Final stack trace and kill 2 min before timeout
  local secondsToKill=$(($REAL_TIMEOUT_SECONDS - $(($REAL_TIMEOUT_SECONDS * 95 / 100)) - $KILL_SECONDS_BEFORE_TIMEOUT))
  if [[ $secondsToKill -lt 0 ]]; then
    secondsToKill=0
  fi
  sleep ${secondsToKill}
  print_stacktraces | tee "$debug_dir/jps-traces.1"

  echo "============================="
  echo "=== WARNING: Killing Flink tests ==="
  echo "============================="
  pkill -P $(<$MAIN_PID_FILE)
  kill $(<$MAIN_PID_FILE)

  exit 42
}

timeout_watchdog &
WATCHDOG_PID=$!

# Run the command with timestamping
( $COMMAND & PID=$! ; echo $PID >$MAIN_PID_FILE ; wait $PID ) | ts | tee $debug_dir/test-output
TEST_EXIT_CODE=${PIPESTATUS[0]}

if [[ "$TEST_EXIT_CODE" == 0 ]]; then
  echo "[INFO] Flink tests passed. Cleaning up."
  kill $WATCHDOG_PID
  rm $debug_dir/test-output
  rm -f $debug_dir/jps-traces.*
fi

exit $TEST_EXIT_CODE