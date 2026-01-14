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

STAGE_CORE="core"
STAGE_FLINK="flink"
STAGE_SPARK="spark3"
STAGE_LAKE="lake"

MODULES_FLINK="\
fluss-flink,\
fluss-flink/fluss-flink-common,\
fluss-flink/fluss-flink-2.2,\
fluss-flink/fluss-flink-1.20,\
"

MODULES_COMMON_SPARK="\
fluss-spark,\
fluss-spark/fluss-spark-common,\
fluss-spark/fluss-spark-ut,\
"

MODULES_SPARK3="\
fluss-spark,\
fluss-spark/fluss-spark-common,\
fluss-spark/fluss-spark-ut,\
fluss-spark/fluss-spark-3.5,\
fluss-spark/fluss-spark-3.4,\
"

# we move Flink legacy version tests to "lake" module for balancing testing time
MODULES_LAKE="\
fluss-flink/fluss-flink-1.19,\
fluss-flink/fluss-flink-1.18,\
fluss-lake,\
fluss-lake/fluss-lake-paimon,\
fluss-lake/fluss-lake-iceberg,\
fluss-lake/fluss-lake-lance
"

function get_test_modules_for_stage() {
    local stage=$1

    local modules_flink=$MODULES_FLINK
    local modules_spark3=$MODULES_SPARK3
    local modules_lake=$MODULES_LAKE
    local negated_flink=\!${MODULES_FLINK//,/,\!}
    local negated_spark=\!${MODULES_COMMON_SPARK//,/,\!}
    local negated_lake=\!${MODULES_LAKE//,/,\!}
    local modules_core="$negated_flink,$negated_spark,$negated_lake"

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $modules_core"
        ;;
        (${STAGE_FLINK})
            echo "-pl fluss-test-coverage,$modules_flink"
        ;;
        (${STAGE_SPARK})
            echo "-Pspark3 -pl fluss-test-coverage,$modules_spark3"
        ;;
        (${STAGE_LAKE})
            echo "-pl fluss-test-coverage,$modules_lake"
        ;;
    esac
}

get_test_modules_for_stage $1
