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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.fluss.config.ConfigOptions.DATALAKE_FORMAT;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for lake enable table. */
class LakeEnableTableITCase extends ClientToServerITCaseBase {

    @Test
    void testCannotEnableDatalakeForTableCreatedBeforeClusterEnabledDatalake() throws Exception {
        String databaseName = "test_db";
        String tableName = "test_table_before_datalake";
        TablePath tablePath = TablePath.of(databaseName, tableName);

        // Disable datalake format for the cluster
        admin.alterClusterConfigs(
                        Collections.singletonList(
                                new AlterConfig(
                                        DATALAKE_FORMAT.key(), null, AlterConfigOpType.SET)))
                .get();
        // Verify cluster now has no datalake format enabled
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getCoordinatorServer()
                                .getCoordinatorService()
                                .getDataLakeFormat())
                .isEqualTo(null);

        // Create database
        admin.createDatabase(databaseName, DatabaseDescriptor.EMPTY, true).get();

        // Create table before cluster enables datalake format
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .build())
                        .distributedBy(3, "c1")
                        .build();
        admin.createTable(tablePath, tableDescriptor, false).get();

        // Verify table was created without datalake format
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getTableConfig().getDataLakeFormat().isPresent()).isFalse();
        assertThat(tableInfo.getTableConfig().isDataLakeEnabled()).isFalse();

        // Enable datalake format for the cluster
        admin.alterClusterConfigs(
                        Collections.singletonList(
                                new AlterConfig(
                                        DATALAKE_FORMAT.key(),
                                        DataLakeFormat.PAIMON.toString(),
                                        AlterConfigOpType.SET)))
                .get();
        // Verify cluster now has datalake format enabled
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getCoordinatorServer()
                                .getCoordinatorService()
                                .getDataLakeFormat())
                .isEqualTo(DataLakeFormat.PAIMON);

        // Try to enable datalake for the table created before cluster enabled datalake
        // This should fail with InvalidAlterTableException
        List<TableChange> enableDatalakeChange =
                Collections.singletonList(TableChange.set(TABLE_DATALAKE_ENABLED.key(), "true"));
        assertThatThrownBy(() -> admin.alterTable(tablePath, enableDatalakeChange, false).get())
                .cause()
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining(
                        "The option 'table.datalake.enabled' cannot be altered for tables that were"
                                + " created before the Fluss cluster enabled datalake.");
    }

    @Test
    void testTableWithExplicitDatalakeFormatCanEnableDatalake() throws Exception {
        String databaseName = "test_db";
        String tableName = "test_table_explicit_format";
        TablePath tablePath = TablePath.of(databaseName, tableName);

        // Create database
        admin.createDatabase(databaseName, DatabaseDescriptor.EMPTY, true).get();

        // Disable datalake format for the cluster
        admin.alterClusterConfigs(
                        Collections.singletonList(
                                new AlterConfig(
                                        DATALAKE_FORMAT.key(), null, AlterConfigOpType.SET)))
                .get();
        // Verify cluster now has no datalake format enabled
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getCoordinatorServer()
                                .getCoordinatorService()
                                .getDataLakeFormat())
                .isEqualTo(null);

        // Create table with explicit datalake format set (even though cluster doesn't set it)
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .build())
                        .distributedBy(3, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                        .build();
        admin.createTable(tablePath, tableDescriptor, false).get();
        // Verify table has datalake format
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getTableConfig().getDataLakeFormat().isPresent()).isTrue();
        assertThat(tableInfo.getTableConfig().getDataLakeFormat().get())
                .isEqualTo(DataLakeFormat.PAIMON);

        // Enable datalake format for the cluster
        admin.alterClusterConfigs(
                        Collections.singletonList(
                                new AlterConfig(
                                        DATALAKE_FORMAT.key(),
                                        DataLakeFormat.PAIMON.toString(),
                                        AlterConfigOpType.SET)))
                .get();
        // Verify cluster now has datalake format enabled
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getCoordinatorServer()
                                .getCoordinatorService()
                                .getDataLakeFormat())
                .isEqualTo(DataLakeFormat.PAIMON);

        // Enable datalake for the table - this should succeed although the table was created
        // before cluster enabled datalake, because the table has explicit datalake format set
        List<TableChange> enableDatalakeChange =
                Collections.singletonList(TableChange.set(TABLE_DATALAKE_ENABLED.key(), "true"));
        admin.alterTable(tablePath, enableDatalakeChange, false).get();
        // Verify datalake is now enabled
        TableInfo updatedTableInfo = admin.getTableInfo(tablePath).get();
        assertThat(updatedTableInfo.getTableConfig().isDataLakeEnabled()).isTrue();
    }
}
