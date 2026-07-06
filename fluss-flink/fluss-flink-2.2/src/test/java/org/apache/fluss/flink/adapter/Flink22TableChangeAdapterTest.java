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

package org.apache.fluss.flink.adapter;

import org.apache.fluss.metadata.TableChange;

import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_DEFINITION_QUERY;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableChangeAdapter} in Flink 2.2. */
public class Flink22TableChangeAdapterTest {

    @Test
    void testConvertModifyDefinitionQuery() {
        String definitionQuery = "SELECT * FROM new_source";
        Optional<List<TableChange>> result =
                TableChangeAdapter.convert(
                        new org.apache.flink.table.catalog.TableChange.ModifyDefinitionQuery(
                                definitionQuery));

        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(1);
        assertThat(result.get().get(0)).isInstanceOf(TableChange.SetOption.class);
        TableChange.SetOption setOption = (TableChange.SetOption) result.get().get(0);
        assertThat(setOption.getKey()).isEqualTo(MATERIALIZED_TABLE_DEFINITION_QUERY.key());
        assertThat(setOption.getValue()).isEqualTo(definitionQuery);
    }

    @Test
    void testConvertUnhandledChangeReturnsEmpty() {
        // ModifyRefreshStatus is converted directly in FlinkConversions, not by this adapter.
        Optional<List<TableChange>> result =
                TableChangeAdapter.convert(
                        new org.apache.flink.table.catalog.TableChange.ModifyRefreshStatus(
                                CatalogMaterializedTable.RefreshStatus.ACTIVATED));
        assertThat(result).isEmpty();
    }
}
