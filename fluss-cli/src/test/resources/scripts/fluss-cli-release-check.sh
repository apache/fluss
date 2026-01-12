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

set -euo pipefail

BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-"localhost:9123"}
CLI_BIN=${CLI_BIN:-"/root/fluss/build-target/bin/fluss-cli.sh"}
TEST_DB=${TEST_DB:-"cli_release_check_$(date +%Y%m%d%H%M%S)"}
CLEANUP=${CLEANUP:-"0"}

# Suppress CLI warnings for clean test output
export FLUSS_CLI_SUPPRESS_WARNINGS=1

TEST_COUNTER=0

print_section() {
  local title=$1
  echo ""
  echo "================================================================================"
  echo "$title"
  echo "================================================================================"
}

run_sql() {
  local sql=$1
  TEST_COUNTER=$((TEST_COUNTER + 1))
  print_section "[TEST ${TEST_COUNTER}] $sql"
  if "$CLI_BIN" sql -b "$BOOTSTRAP_SERVERS" -e "$sql"; then
    echo "Result: PASS"
  else
    echo "Result: FAIL"
    return 1
  fi
}

run_optional() {
  local sql=$1
  TEST_COUNTER=$((TEST_COUNTER + 1))
  print_section "[TEST ${TEST_COUNTER}] (optional) $sql"
  if "$CLI_BIN" sql -b "$BOOTSTRAP_SERVERS" -e "$sql"; then
    echo "Result: PASS"
  else
    echo "Result: FAIL (optional)"
  fi
}

echo "== Fluss CLI release check =="
echo "Bootstrap: $BOOTSTRAP_SERVERS"
echo "Database: $TEST_DB"

echo "-- Core metadata --"
run_sql "SHOW SERVERS"
run_sql "SHOW DATABASES"

run_sql "CREATE DATABASE IF NOT EXISTS ${TEST_DB}"
run_sql "SHOW DATABASE EXISTS ${TEST_DB}"
run_sql "SHOW DATABASE ${TEST_DB}"

run_sql "USING ${TEST_DB}"
run_sql "SHOW TABLES FROM ${TEST_DB}"

run_sql "CREATE TABLE ${TEST_DB}.pk_table (id INT, name STRING, score DOUBLE, ts TIMESTAMP, PRIMARY KEY (id))"
run_sql "CREATE TABLE ${TEST_DB}.log_table (id INT, val STRING)"
run_sql "CREATE TABLE ${TEST_DB}.complex_table (id INT, tags ARRAY<STRING>, props MAP<STRING, INT>, profile ROW<name STRING, age INT>, PRIMARY KEY (id))"

run_sql "SHOW TABLE EXISTS ${TEST_DB}.pk_table"
run_sql "SHOW TABLE SCHEMA ${TEST_DB}.pk_table"
run_sql "SHOW CREATE TABLE ${TEST_DB}.pk_table"

run_sql "INSERT INTO ${TEST_DB}.pk_table VALUES (1, 'Alice', 1.5, '2024-01-01 00:00:00')"
run_sql "UPSERT INTO ${TEST_DB}.pk_table VALUES (1, 'Alice2', 2.5, '2024-01-02 00:00:00')"
run_sql "UPDATE ${TEST_DB}.pk_table SET score = 3.0 WHERE id = 1"
run_sql "SELECT * FROM ${TEST_DB}.pk_table WHERE id = 1"
run_sql "DELETE FROM ${TEST_DB}.pk_table WHERE id = 1"

run_sql "INSERT INTO ${TEST_DB}.log_table VALUES (1, 'v1')"
run_sql "SELECT * FROM ${TEST_DB}.log_table"

run_sql "INSERT INTO ${TEST_DB}.complex_table VALUES (1, ARRAY['a','b'], MAP['k1',1,'k2',2], ROW('Bob', 30))"
run_sql "SELECT * FROM ${TEST_DB}.complex_table WHERE id = 1"

run_optional "SHOW KV SNAPSHOTS FROM ${TEST_DB}.pk_table"
run_optional "SHOW KV SNAPSHOT METADATA FROM ${TEST_DB}.pk_table BUCKET 0 SNAPSHOT 0"
run_optional "SHOW LAKE SNAPSHOT FROM ${TEST_DB}.pk_table"
run_optional "SHOW OFFSETS FROM ${TEST_DB}.pk_table BUCKETS (0) AT EARLIEST"

echo ""
echo "-- Server tag operations --"
run_sql "ALTER SERVER TAG ADD TEMPORARY_OFFLINE TO (1)"
run_sql "SHOW SERVERS"
run_sql "ALTER SERVER TAG REMOVE TEMPORARY_OFFLINE FROM (1)"
run_sql "SHOW SERVERS"

run_sql "ALTER SERVER TAG ADD TEMPORARY_OFFLINE TO (1,2,3)"
run_sql "SHOW SERVERS"
run_sql "ALTER SERVER TAG REMOVE TEMPORARY_OFFLINE FROM (1,2,3)"
run_sql "SHOW SERVERS"

if [ "$CLEANUP" = "1" ]; then
  run_sql "DROP DATABASE ${TEST_DB}"
fi

echo "== Release check completed =="
