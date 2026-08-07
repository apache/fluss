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

package org.apache.fluss.flink.utils;

import org.apache.fluss.metadata.TableChange;

import org.apache.flink.table.catalog.TableChange.ModifyDefinitionQuery;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_DEFINITION_QUERY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link FlinkConversions} against the Flink 2.x-only {@link ModifyDefinitionQuery} table
 * change.
 */
class Flink22FlinkConversionsTest {

    @Test
    void testConvertModifyDefinitionQuery() {
        String newDefinitionQuery = "SELECT order_id, orig_ts FROM new_source";

        List<TableChange> flussTableChanges =
                FlinkConversions.toFlussTableChanges(new ModifyDefinitionQuery(newDefinitionQuery));

        assertThat(flussTableChanges).hasSize(1);
        assertThat(flussTableChanges.get(0)).isInstanceOf(TableChange.SetOption.class);
        TableChange.SetOption setOption = (TableChange.SetOption) flussTableChanges.get(0);
        assertThat(setOption.getKey()).isEqualTo(MATERIALIZED_TABLE_DEFINITION_QUERY.key());
        assertThat(setOption.getValue()).isEqualTo(newDefinitionQuery);
    }
}
