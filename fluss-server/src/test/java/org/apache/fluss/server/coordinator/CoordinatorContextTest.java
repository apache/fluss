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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoordinatorContext}. */
class CoordinatorContextTest {

    @Test
    void testGetLakeTableCount() {
        CoordinatorContext context = new CoordinatorContext();

        // Initially, there should be no tables
        assertThat(context.allTables()).isEmpty();
        assertThat(context.getLakeTableCount()).isEqualTo(0);

        // Add a non-lake table
        TableInfo nonLakeTable = createTableInfo(1L, TablePath.of("db1", "table1"), false);
        context.putTablePath(1L, nonLakeTable.getTablePath());
        context.putTableInfo(nonLakeTable);

        assertThat(context.allTables()).hasSize(1);
        assertThat(context.getLakeTableCount()).isEqualTo(0);

        // Add a lake table
        TableInfo lakeTable = createTableInfo(2L, TablePath.of("db1", "table2"), true);
        context.putTablePath(2L, lakeTable.getTablePath());
        context.putTableInfo(lakeTable);

        assertThat(context.allTables()).hasSize(2);
        assertThat(context.getLakeTableCount()).isEqualTo(1);

        // Add another lake table
        TableInfo lakeTable2 = createTableInfo(3L, TablePath.of("db2", "table3"), true);
        context.putTablePath(3L, lakeTable2.getTablePath());
        context.putTableInfo(lakeTable2);

        assertThat(context.allTables()).hasSize(3);
        assertThat(context.getLakeTableCount()).isEqualTo(2);
    }

    private TableInfo createTableInfo(long tableId, TablePath tablePath, boolean isLake) {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ZERO)
                        .property(TABLE_DATALAKE_ENABLED, isLake)
                        .distributedBy(1)
                        .build();

        return TableInfo.of(
                tablePath,
                tableId,
                1,
                tableDescriptor,
                DEFAULT_REMOTE_DATA_DIR,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }
}
