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

package org.apache.fluss.lake.paimon.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end IT case for looking up expired Fluss partitions from Paimon. */
class HistoricalPartitionLookupITCase extends FlinkPaimonTieringTestBase {

    private static final String EXPIRED_PARTITION_NAME = "20240101";
    private static final int INITIAL_PARTITION_RETENTION = 100000;
    private static final int EXPIRED_PARTITION_RETENTION = 1;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    @BeforeAll
    protected static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());
    }

    @Test
    void testLookupExpiredPartitionFromPaimon() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "historical_lookup_pk");
        Schema oldSchema = partitionedPkSchema();
        long tableId = createTable(tablePath, partitionedPkDescriptor(oldSchema));

        // Keep the initial retention wide enough so this old partition can be created and written
        // through the normal Fluss path before it is treated as historical.
        PartitionSpec expiredPartitionSpec = partitionSpec(EXPIRED_PARTITION_NAME);
        admin.createPartition(tablePath, expiredPartitionSpec, false).get();
        long partitionId = getPartitionId(tablePath, EXPIRED_PARTITION_NAME);
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, partitionId);

        InternalRow expectedOldRow = row(1, EXPIRED_PARTITION_NAME, "Alice");
        writeRows(tablePath, Collections.singletonList(expectedOldRow), false);

        TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshots(Collections.singleton(tableBucket));

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(tableBucket, 1);
        } finally {
            jobClient.cancel().get();
        }

        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "extra",
                                        DataTypes.STRING(),
                                        "extra column",
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();
        Schema evolvedSchema = evolvedPartitionedPkSchema();

        InternalRow expectedNewRow = row(2, EXPIRED_PARTITION_NAME, "Bob", "new-value");
        writeRows(tablePath, Collections.singletonList(expectedNewRow), false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshots(Collections.singleton(tableBucket));

        jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(tableBucket, 2);
        } finally {
            jobClient.cancel().get();
        }

        // After the row is tiered to Paimon, shrink the retention and remove the Fluss partition.
        // A fresh lookup client should then route the missing old partition to historical lookup.
        // The returned Paimon rows should be decoded with the evolved Fluss schema.
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key(),
                                        String.valueOf(EXPIRED_PARTITION_RETENTION))),
                        false)
                .get();
        admin.dropPartition(tablePath, expiredPartitionSpec, true).get();
        waitUntilPartitionDropped(tablePath, EXPIRED_PARTITION_NAME);

        InternalRow lookupRow =
                lookupWithFreshConnection(tablePath, row(1, EXPIRED_PARTITION_NAME));
        assertThatRow(lookupRow)
                .withSchema(evolvedSchema.getRowType())
                .isEqualTo(row(1, EXPIRED_PARTITION_NAME, "Alice", null));

        lookupRow = lookupWithFreshConnection(tablePath, row(2, EXPIRED_PARTITION_NAME));
        assertThatRow(lookupRow).withSchema(evolvedSchema.getRowType()).isEqualTo(expectedNewRow);
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }

    private static Schema partitionedPkSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .column("name", DataTypes.STRING())
                .primaryKey("id", "dt")
                .build();
    }

    private static Schema evolvedPartitionedPkSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .column("name", DataTypes.STRING())
                .column("extra", DataTypes.STRING())
                .primaryKey("id", "dt")
                .build();
    }

    private static TableDescriptor partitionedPkDescriptor(Schema schema) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(1, "id")
                .partitionedBy("dt")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "dt")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.DAY)
                .property(
                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION,
                        INITIAL_PARTITION_RETENTION)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "UTC")
                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                .build();
    }

    private static PartitionSpec partitionSpec(String partitionName) {
        return new PartitionSpec(Collections.singletonMap("dt", partitionName));
    }

    private static long getPartitionId(TablePath tablePath, String partitionName) throws Exception {
        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        for (PartitionInfo partitionInfo : partitionInfos) {
            if (partitionName.equals(partitionInfo.getPartitionName())) {
                return partitionInfo.getPartitionId();
            }
        }
        throw new IllegalStateException("Partition " + partitionName + " does not exist.");
    }

    private static void waitUntilPartitionDropped(TablePath tablePath, String partitionName) {
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(admin.listPartitionInfos(tablePath).get())
                                .noneMatch(p -> partitionName.equals(p.getPartitionName())));
    }

    private static InternalRow lookupWithFreshConnection(TablePath tablePath, InternalRow lookupKey)
            throws Exception {
        try (Connection lookupConn = ConnectionFactory.createConnection(clientConf);
                Table table = lookupConn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            return lookuper.lookup(lookupKey).get().getSingletonRow();
        }
    }
}
