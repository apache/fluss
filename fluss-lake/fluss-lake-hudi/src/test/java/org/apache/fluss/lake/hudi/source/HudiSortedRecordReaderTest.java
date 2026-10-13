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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.hudi.HudiLakeCatalog;
import org.apache.fluss.lake.hudi.utils.HudiTableInfo;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HudiSortedRecordReader}. */
class HudiSortedRecordReaderTest {

    @TempDir private File tempWarehouseDir;

    private Configuration hudiConfig;

    @BeforeEach
    void setUp() {
        hudiConfig = new Configuration();
        hudiConfig.setString("catalog.path", tempWarehouseDir.toURI().toString());
        hudiConfig.setString("mode", "dfs");
    }

    @Test
    void testCanSortByRecordKeyRequiresProjectedKey() throws Exception {
        TablePath tablePath = TablePath.of("db1", "single_pk_table");
        createTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build(),
                TableDescriptor.builder().distributedBy(4));

        try (HudiTableInfo hudiTableInfo = HudiTableInfo.create(tablePath, hudiConfig)) {
            assertThat(HudiSortedRecordReader.canSortByRecordKey(hudiTableInfo, null)).isTrue();
            assertThat(HudiSortedRecordReader.canSortByRecordKey(hudiTableInfo, project(0)))
                    .isTrue();
            assertThat(HudiSortedRecordReader.canSortByRecordKey(hudiTableInfo, project(1)))
                    .isFalse();
            assertThat(HudiSortedRecordReader.canSortByRecordKey(hudiTableInfo, project(1, 0)))
                    .isTrue();
        }
    }

    @Test
    void testCanSortByRecordKeyRequiresAllProjectedKeys() throws Exception {
        TablePath tablePath = TablePath.of("db1", "composite_pk_table");
        createTable(
                tablePath,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .build(),
                TableDescriptor.builder().distributedBy(4).partitionedBy("dt"));

        try (HudiTableInfo hudiTableInfo = HudiTableInfo.create(tablePath, hudiConfig)) {
            assertThat(HudiSortedRecordReader.canSortByRecordKey(hudiTableInfo, project(0, 1)))
                    .isTrue();
            assertThat(HudiSortedRecordReader.canSortByRecordKey(hudiTableInfo, project(0)))
                    .isFalse();
            assertThat(HudiSortedRecordReader.canSortByRecordKey(hudiTableInfo, project(1)))
                    .isFalse();
        }
    }

    private void createTable(
            TablePath tablePath, Schema schema, TableDescriptor.Builder tableBuilder)
            throws Exception {
        TableDescriptor tableDescriptor =
                tableBuilder.schema(schema).property("hudi.precombine.field", "id").build();
        try (HudiLakeCatalog catalog = new HudiLakeCatalog(hudiConfig)) {
            catalog.createTable(tablePath, tableDescriptor, new TestingLakeCatalogContext());
        }
    }

    private static int[][] project(int... fields) {
        int[][] project = new int[fields.length][];
        for (int i = 0; i < fields.length; i++) {
            project[i] = new int[] {fields[i]};
        }
        return project;
    }
}
