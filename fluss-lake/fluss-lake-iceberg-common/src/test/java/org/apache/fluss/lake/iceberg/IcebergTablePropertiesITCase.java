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

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Admin RPC tests for Fluss-only and native Iceberg property changes. */
class IcebergTablePropertiesITCase extends FlinkIcebergTieringTestBase {

    private static final String RETENTION = "table.auto-partition.num-retention";
    private static final String ENABLED = "table.datalake.enabled";
    private static final String TARGET_SIZE = "iceberg.write.target-file-size-bytes";

    @Test
    void testDisabledPkTableDoesNotRequireLakeTable() throws Exception {
        TablePath path = createTable("disabled_pk_properties", false);

        alter(path, TableChange.set(RETENTION, "3"));

        assertThat(retention(path)).isEqualTo(3);
        assertThat(icebergCatalog.tableExists(toIceberg(path))).isFalse();
    }

    @Test
    void testMixedChangesOnlyUpdateNativeIcebergProperties() throws Exception {
        TablePath path = createTable("mixed_pk_properties", true);

        alter(
                path,
                TableChange.set(RETENTION, "3"),
                TableChange.set(TARGET_SIZE, "1048576"),
                TableChange.set("k1", "v1"));

        assertThat(retention(path)).isEqualTo(3);
        Table lakeTable = icebergCatalog.loadTable(toIceberg(path));
        assertThat(lakeTable.properties())
                .containsEntry("write.target-file-size-bytes", "1048576")
                .containsEntry("fluss." + RETENTION, "1")
                .doesNotContainKey("fluss.k1");

        alter(path, TableChange.reset(TARGET_SIZE), TableChange.reset("k1"));
        assertThat(icebergCatalog.loadTable(toIceberg(path)).properties())
                .doesNotContainKey("write.target-file-size-bytes");
        assertThat(admin.getTableInfo(path).get().getCustomProperties().toMap())
                .doesNotContainKeys(TARGET_SIZE, "k1");
    }

    @Test
    void testReenableAfterFlussConfigurationChanged() throws Exception {
        TablePath path = createTable("reenable_pk_properties", true);
        alter(path, TableChange.set(ENABLED, "false"));
        alter(path, TableChange.set(RETENTION, "3"));

        assertThat(icebergCatalog.loadTable(toIceberg(path)).properties())
                .containsEntry("fluss." + RETENTION, "1");

        alter(path, TableChange.set(ENABLED, "true"));

        assertThat(retention(path)).isEqualTo(3);
        assertThat(admin.getTableInfo(path).get().getTableConfig().isDataLakeEnabled()).isTrue();
        assertThat(icebergCatalog.loadTable(toIceberg(path)).properties())
                .containsEntry("fluss." + RETENTION, "1");
    }

    @Test
    void testDeletedLakeDoesNotBlockFlussConfiguration() throws Exception {
        TablePath path = createTable("deleted_lake_properties", true);
        assertThat(icebergCatalog.dropTable(toIceberg(path))).isTrue();

        alter(path, TableChange.set(RETENTION, "3"));
        assertThat(retention(path)).isEqualTo(3);

        assertThatThrownBy(() -> alter(path, TableChange.set(TARGET_SIZE, "1048576")))
                .cause()
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Lake table doesn't exist for lake-enabled table");
    }

    private TablePath createTable(String name, boolean enabled) throws Exception {
        TablePath path = TablePath.of("fluss", name);
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.BIGINT())
                                        .column("pt", DataTypes.STRING())
                                        .primaryKey("id", "pt")
                                        .build())
                        .distributedBy(1, "id")
                        .partitionedBy("pt")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, enabled)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "pt")
                        .property("table.auto-partition.time-unit", "HOUR")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 1)
                        .build();
        admin.createTable(path, descriptor, false).get();
        return path;
    }

    private int retention(TablePath path) throws Exception {
        return admin.getTableInfo(path)
                .get()
                .getTableConfig()
                .getAutoPartitionStrategy()
                .numToRetain();
    }

    private void alter(TablePath path, TableChange... changes) throws Exception {
        admin.alterTable(path, Arrays.asList(changes), false).get();
    }
}
