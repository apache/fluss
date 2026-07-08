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
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HudiLakeSource}. */
class HudiLakeSourceTest {

    @TempDir private File tempWarehouseDir;

    @Test
    void testCreatePlannerReturnsHudiSplitPlanner() throws Exception {
        HudiLakeSource source =
                new HudiLakeSource(new Configuration(), TablePath.of("db1", "table1"));

        assertThat(source.createPlanner(() -> 1L)).isInstanceOf(HudiSplitPlanner.class);
    }

    @Test
    void testGetSplitSerializerReturnsHudiSplitSerializer() {
        HudiLakeSource source =
                new HudiLakeSource(new Configuration(), TablePath.of("db1", "table1"));

        assertThat(source.getSplitSerializer()).isInstanceOf(HudiSplitSerializer.class);
    }

    @Test
    void testWithEmptyFiltersReturnsEmptyPushDownResult() {
        HudiLakeSource source =
                new HudiLakeSource(new Configuration(), TablePath.of("db1", "table1"));

        LakeSource.FilterPushDownResult result = source.withFilters(Collections.emptyList());

        assertThat(result.acceptedPredicates()).isEmpty();
        assertThat(result.remainingPredicates()).isEmpty();
    }

    @Test
    void testWithLimitIsExplicitlyUnsupported() {
        HudiLakeSource source =
                new HudiLakeSource(new Configuration(), TablePath.of("db1", "table1"));

        assertThatThrownBy(() -> source.withLimit(1))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("limit");
    }

    @Test
    void testReaderContextDoesNotRequireSortedRecordsByDefault() {
        LakeSource.ReaderContext<HudiSplit> context = () -> null;

        assertThat(context.requireSortedRecords()).isFalse();
    }

    @Test
    void testCreateRecordReaderUsesSortedReaderWhenRequired() throws Exception {
        Configuration hudiConfig = hudiConfig();
        TablePath tablePath = TablePath.of("db1", "sorted_required_pk_table");
        createPkTable(hudiConfig, tablePath);

        HudiLakeSource source = new HudiLakeSource(hudiConfig, tablePath);

        RecordReader recordReader = source.createRecordReader(sortedReaderContext(null));

        assertThat(recordReader).isInstanceOf(HudiSortedRecordReader.class);
    }

    @Test
    void testCreateRecordReaderFailsWhenSortedReaderCanNotKeepRecordKeyOrder() throws Exception {
        Configuration hudiConfig = hudiConfig();
        TablePath tablePath = TablePath.of("db1", "sorted_required_projected_pk_table");
        createPkTable(hudiConfig, tablePath);

        HudiLakeSource source = new HudiLakeSource(hudiConfig, tablePath);
        source.withProject(project(1));

        assertThatThrownBy(() -> source.createRecordReader(sortedReaderContext(null)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Fail to create Hudi record reader")
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    private Configuration hudiConfig() {
        Configuration hudiConfig = new Configuration();
        hudiConfig.setString("catalog.path", tempWarehouseDir.toURI().toString());
        hudiConfig.setString("mode", "dfs");
        return hudiConfig;
    }

    private static LakeSource.ReaderContext<HudiSplit> sortedReaderContext(
            @Nullable HudiSplit hudiSplit) {
        return new LakeSource.ReaderContext<HudiSplit>() {
            @Nullable
            @Override
            public HudiSplit lakeSplit() {
                return hudiSplit;
            }

            @Override
            public boolean requireSortedRecords() {
                return true;
            }
        };
    }

    private static void createPkTable(Configuration hudiConfig, TablePath tablePath)
            throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(4)
                        .property("hudi.precombine.field", "id")
                        .build();
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
