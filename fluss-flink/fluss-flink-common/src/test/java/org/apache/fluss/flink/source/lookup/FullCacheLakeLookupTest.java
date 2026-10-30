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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KvBatchStrategy;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.time.Duration;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests non-partitioned lake-enabled lookup using Fluss snapshots and changelog only. */
class FullCacheLakeLookupTest {

    // The test lake plugin uses LANCE and does not need an external lake or tiering job.
    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.LANCE))
                    .setNumOfTabletServers(1)
                    .build();

    @TempDir private Path tempDir;

    @ParameterizedTest
    @EnumSource(KvBatchStrategy.class)
    void testNonPartitionedLakeTable(KvBatchStrategy strategy) throws Exception {
        Configuration config = new Configuration(CLUSTER.getClientConfig());
        config.set(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR, tempDir.toString());
        config.set(ConfigOptions.CLIENT_SCANNER_KV_BATCH_STRATEGY, strategy);
        TablePath path = TablePath.of("full_cache_lake", "dimension_" + strategy);
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("id", DataTypes.INT())
                                                    .column("name", DataTypes.STRING())
                                                    .primaryKey("id")
                                                    .build())
                                    .distributedBy(2, "id")
                                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                                    .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                                    .build(),
                            false)
                    .get();
            try (Table table = connection.getTable(path)) {
                TableInfo info = table.getTableInfo();
                // The writes below may hit only one bucket, but snapshot triggering visits all.
                for (int bucket = 0; bucket < info.getNumBuckets(); bucket++) {
                    CLUSTER.waitUntilAllReplicaReady(new TableBucket(info.getTableId(), bucket));
                }
                assertThat(info.getTableConfig().isDataLakeEnabled()).isTrue();
                assertThat(info.getTableConfig().getDataLakeFormat())
                        .contains(DataLakeFormat.LANCE);
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(row(1, "snapshot"));
                writer.upsert(row(2, "deleted"));
                writer.flush();
                CLUSTER.triggerAndWaitSnapshot(path);
                writer.upsert(row(1, "caught-up"));
                writer.delete(row(2, "deleted"));
                writer.flush();
                KeyEncoder encoder =
                        KeyEncoder.ofPrimaryKeyEncoder(
                                info.getRowType(),
                                info.getPhysicalPrimaryKeys(),
                                info.getTableConfig(),
                                info.isDefaultBucketKey());
                RowDecoder decoder =
                        RowDecoder.create(
                                info.getTableConfig().getKvFormat(),
                                info.getRowType().getChildren().toArray(new DataType[0]));
                try (FullCacheLookupTable lookupTable =
                        new FullCacheLookupTable(
                                config,
                                path,
                                2,
                                new FullCacheLookupOptions(
                                        new org.apache.flink.configuration.Configuration()))) {
                    lookupTable.open(0, 1, new UnregisteredMetricsGroup());
                    byte[] key = encoder.encodeKey(row(1, null));
                    assertThat(
                                    decoder.decode(lookupTable.lookup(key, false).get(0))
                                            .getString(1)
                                            .toString())
                            .isEqualTo("caught-up");
                    assertThat(lookupTable.lookup(encoder.encodeKey(row(2, null)), false))
                            .isEmpty();
                    writer.upsert(row(1, "updated"));
                    writer.flush();
                    retry(
                            Duration.ofSeconds(30),
                            () ->
                                    assertThat(
                                                    decoder.decode(
                                                                    lookupTable
                                                                            .lookup(key, false)
                                                                            .get(0))
                                                            .getString(1)
                                                            .toString())
                                            .isEqualTo("updated"));
                }
            }
        }
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }
}
