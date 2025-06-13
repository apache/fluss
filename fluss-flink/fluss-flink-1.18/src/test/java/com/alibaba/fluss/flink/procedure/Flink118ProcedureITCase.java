/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.procedure;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for authorization in Flink 1.18. */
public class Flink118ProcedureITCase extends FlinkProcedureITCase {

    @Test
    void testIndexArgument() throws Exception {}

    @Test
    void testLackParams() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.list_acl('ANY', 'ANY', 'ANY')",
                                                        CATALOG_NAME))
                                        .wait())
                .hasMessageContaining(
                        "No match found for function signature list_acl(<CHARACTER>, <CHARACTER>, <CHARACTER>)");
    }
}
