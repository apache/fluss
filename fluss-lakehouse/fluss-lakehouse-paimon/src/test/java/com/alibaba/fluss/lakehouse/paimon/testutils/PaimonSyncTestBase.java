/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.testutils;

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;
import com.alibaba.fluss.lakehouse.paimon.sink.NewTablesAddedPaimonListener;
import com.alibaba.fluss.lakehouse.paimon.sink.PaimonDataBaseSyncSinkBuilder;
import com.alibaba.fluss.lakehouse.paimon.source.FlussDatabaseSyncSource;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Map;

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;

/** Test base for sync to paimon. */
public class PaimonSyncTestBase extends FlinkPaimonTestBase {

    protected static final String CATALOG_NAME = "testcatalog";
    protected static final String DEFAULT_DB = "fluss";
    protected StreamExecutionEnvironment execEnv;

    protected static Catalog paimonCatalog;

    @BeforeAll
    protected static void beforeAll() {
        FlinkPaimonTestBase.beforeAll();
        paimonCatalog = getPaimonCatalog();
    }

    @BeforeEach
    protected void beforeEach() {
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        execEnv.setParallelism(2);
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.enableCheckpointing(1000);
    }

    protected PaimonDataBaseSyncSinkBuilder getDatabaseSyncSinkBuilder(
            StreamExecutionEnvironment execEnv) {
        Configuration configuration = FlinkPaimonTestBase.FLUSS_CLUSTER_EXTENSION.getClientConfig();
        FlussDatabaseSyncSource flussDatabaseSyncSource =
                FlussDatabaseSyncSource.newBuilder(configuration)
                        .withNewTableAddedListener(
                                new NewTablesAddedPaimonListener(
                                        Configuration.fromMap(
                                                FlinkPaimonTestBase.getPaimonCatalogConf())))
                        .build();

        DataStreamSource<MultiplexCdcRecord> input =
                execEnv.fromSource(
                                flussDatabaseSyncSource,
                                WatermarkStrategy.noWatermarks(),
                                "flinkSyncDatabaseSource")
                        // limit resource usage
                        .setParallelism(2);
        Map<String, String> paimonCatalogConf = FlinkPaimonTestBase.getPaimonCatalogConf();

        return new PaimonDataBaseSyncSinkBuilder(paimonCatalogConf, configuration).withInput(input);
    }

    protected static Catalog getPaimonCatalog() {
        Map<String, String> catalogOptions = FlinkPaimonTestBase.getPaimonCatalogConf();
        return CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(catalogOptions)));
    }

    protected Replica getLeaderReplica(TableBucket tableBucket) {
        return FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
    }

    protected long createLogTable(TablePath tablePath) throws Exception {
        return createLogTable(tablePath, 1);
    }

    protected long createLogTable(TablePath tablePath, int bucketNum) throws Exception {
        return createLogTable(tablePath, bucketNum, false);
    }

    protected long createLogTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("a", DataTypes.INT()).column("b", DataTypes.STRING());

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(bucketNum, "a")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");

        if (isPartitioned) {
            schemaBuilder.column("c", DataTypes.STRING());
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy("c");
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    protected long createPkTable(TablePath tablePath) throws Exception {
        return createPkTable(tablePath, 1);
    }

    protected long createPkTable(TablePath tablePath, int bucketNum) throws Exception {
        TableDescriptor table1Descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .primaryKey("a")
                                        .build())
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .build();
        return createTable(tablePath, table1Descriptor);
    }

    protected void assertReplicaStatus(
            TableBucket tb, long expectedLogStartOffset, long expectedLogEndOffset) {
        retry(
                Duration.ofMinutes(2),
                () -> {
                    Replica replica = getLeaderReplica(tb);
                    // datalake snapshot id should be updated
                    assertThat(replica.getLogTablet().getLakeTableSnapshotId())
                            .isGreaterThanOrEqualTo(0);
                    assertThat(replica.getLakeLogEndOffset()).isEqualTo(expectedLogEndOffset);
                    assertThat(replica.getLogTablet().getLakeLogStartOffset())
                            .isEqualTo(expectedLogStartOffset);
                });
    }

    protected void assertReplicaStatus(
            TablePath tablePath,
            long tableId,
            int bucketCount,
            boolean isPartitioned,
            Map<TableBucket, Long> expectedLogEndOffset) {
        if (isPartitioned) {
            Map<Long, String> partitionById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath);
            for (Long partitionId : partitionById.keySet()) {
                for (int i = 0; i < bucketCount; i++) {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
                    assertReplicaStatus(tableBucket, expectedLogEndOffset.get(tableBucket));
                }
            }
        } else {
            for (int i = 0; i < bucketCount; i++) {
                TableBucket tableBucket = new TableBucket(tableId, i);
                assertReplicaStatus(tableBucket, expectedLogEndOffset.get(tableBucket));
            }
        }
    }

    protected void assertReplicaStatus(TableBucket tb, long expectedLogEndOffset) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Replica replica = getLeaderReplica(tb);
                    // datalake snapshot id should be updated
                    assertThat(replica.getLogTablet().getLakeTableSnapshotId())
                            .isGreaterThanOrEqualTo(0);
                    assertThat(replica.getLakeLogEndOffset()).isEqualTo(expectedLogEndOffset);
                });
    }

    protected void waitUtilBucketSynced(
            TablePath tablePath, long tableId, int bucketCount, boolean isPartition) {
        if (isPartition) {
            Map<Long, String> partitionById = waitUntilPartitions(tablePath);
            for (Long partitionId : partitionById.keySet()) {
                for (int i = 0; i < bucketCount; i++) {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
                    waitUtilBucketSynced(tableBucket);
                }
            }
        } else {
            for (int i = 0; i < bucketCount; i++) {
                TableBucket tableBucket = new TableBucket(tableId, i);
                waitUtilBucketSynced(tableBucket);
            }
        }
    }

    protected void waitUtilBucketSynced(TableBucket tb) {
        waitUtil(
                () -> {
                    Replica replica = getLeaderReplica(tb);
                    return replica.getLogTablet().getLakeTableSnapshotId() >= 0;
                },
                Duration.ofMinutes(2),
                "bucket " + tb + "not synced");
    }

    protected Object[] rowValues(Object[] values, @Nullable String partition) {
        if (partition == null) {
            return values;
        } else {
            Object[] newValues = new Object[values.length + 1];
            System.arraycopy(values, 0, newValues, 0, values.length);
            newValues[values.length] = partition;
            return newValues;
        }
    }
}
