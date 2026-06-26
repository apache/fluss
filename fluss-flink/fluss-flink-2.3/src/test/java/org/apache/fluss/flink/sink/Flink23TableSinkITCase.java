/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.sink;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.api.BeforeEach;

/** IT case for {@link FlinkTableSink} in Flink 2.3. */
public class Flink23TableSinkITCase extends FlinkTableSinkITCase {

    /**
     * Flink 2.3 introduces {@link ExecutionConfigOptions#TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT}
     * (default {@code true}). In {@code FlinkChangelogModeInferenceProgram} this triggers a "upsert
     * key differs from primary key" ValidationException for partial upserts such as {@code INSERT
     * INTO pk_table(a, b) VALUES ...} where the query carries no upsert key. The parent class
     * {@link FlinkTableSinkITCase#testPartialUpsert()} (and its siblings) rely on Fluss's own
     * partial-update handling at the sink layer, so disable the option here to keep those tests
     * reachable.
     */
    @BeforeEach
    void beforeEach() {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT, false);
    }
}
