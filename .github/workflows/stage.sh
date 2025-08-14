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

MODULES_FLINK="\
fluss-flink,\
fluss-flink/fluss-flink-common,\
fluss-flink/fluss-flink-1.20,\
fluss-flink/fluss-flink-2.1,\
fluss-flink/fluss-flink-1.19,\
fluss-flink/fluss-flink-1.18,\
fluss-lake,\
fluss-lake/fluss-lake-paimon,\
fluss-lake/fluss-lake-iceberg,\
fluss-lake/fluss-lake-lance\
"

# modules that are only built with JDK 11
JDK_11_ONLY="fluss-flink/fluss-flink-2.1"

function get_test_modules_for_stage() {
    local stage=$1
    local jdk_version=$2

    local modules_flink=$MODULES_FLINK


    local included_modules=""
    local excluded_modules=""

    case ${stage} in
        (${STAGE_CORE})
            # For core, we exclude all flink modules
            excluded_modules="$modules_flink";
        ;;
        (${STAGE_FLINK})
            included_modules="fluss-test-coverage,$modules_flink"
        ;;
    esac

    # Add JDK 11 only modules to exclusion list if we're using JDK 8
    if [[ "$jdk_version" == "8" ]]; then
        if [[ -n "$excluded_modules" ]]; then
            excluded_modules="$excluded_modules,$JDK_11_ONLY"
        else
            excluded_modules="$JDK_11_ONLY"
        fi
    fi

    local result="$included_modules"
    if [[ -n "$excluded_modules" ]]; then
        if [[ -n "$result" ]]; then
            result="$result,\!${excluded_modules//,/,\!}"
        else
            result=\!${excluded_modules//,/,\!}
        fi
    fi


    if [[ -n "$result" ]]; then
           echo "-pl $result"
       else
           # If both included_modules and excluded_modules are empty,
           # don't specify any module restrictions (build all modules)
           echo ""
       fi
}

get_test_modules_for_stage $1 $2
