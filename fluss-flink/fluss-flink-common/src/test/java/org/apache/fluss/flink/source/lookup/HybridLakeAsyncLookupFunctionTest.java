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

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.tiering.source.TieringTestBase;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.lake.values.TestingPaimonLakeStoragePlugin;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.flink.source.lookup.LookupNormalizer.createPrimaryKeyLookupNormalizer;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HybridLakeAsyncLookupFunction}. */
class HybridLakeAsyncLookupFunctionTest extends TieringTestBase {

    private static final String EXISTING_PARTITION = "2026";
    private static final String MISSING_PARTITION = "1900";
    private static final TablePath TABLE_PATH =
            TablePath.of(DEFAULT_DB, "hybrid-lake-lookup-table");
    private static final Schema TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("date", DataTypes.STRING())
                    .primaryKey("id", "date")
                    .build();
    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(TABLE_SCHEMA)
                    .distributedBy(1)
                    .partitionedBy("date")
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                    .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                    .build();

    private HybridLakeAsyncLookupFunction lookupFunction;

    @BeforeEach
    void setUp() throws Exception {
        TestingPaimonLakeStoragePlugin.resetLookupFunction();
        admin.createTable(TABLE_PATH, TABLE_DESCRIPTOR, true).get();
        admin.createPartition(TABLE_PATH, partitionSpec(EXISTING_PARTITION), true).get();
        Map<String, Long> partitionIds =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(TABLE_PATH, 1);
        long tableId = admin.getTableInfo(TABLE_PATH).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(
                tableId, partitionIds.get(EXISTING_PARTITION));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (lookupFunction != null) {
            lookupFunction.close();
            lookupFunction = null;
        }
        TestingPaimonLakeStoragePlugin.resetLookupFunction();
    }

    @Test
    void testLookupExistingFlussPartitionWithoutLakeFallback() throws Exception {
        AtomicInteger lakeLookupCount = new AtomicInteger();
        TestingPaimonLakeStoragePlugin.setLookupFunction(
                (key, context) -> {
                    lakeLookupCount.incrementAndGet();
                    return row(1, "lake", EXISTING_PARTITION);
                });
        writeRow(row(1, "fluss", EXISTING_PARTITION));
        openLookupFunction(Duration.ofSeconds(5));

        assertThat(lookup(1, EXISTING_PARTITION))
                .singleElement()
                .extracting(RowData::toString)
                .isEqualTo("+I(1,fluss,2026)");
        assertThat(lookup(2, EXISTING_PARTITION)).isEmpty();
        assertThat(lakeLookupCount).hasValue(0);
    }

    @Test
    void testFallbackToLakeForMissingPartition() throws Exception {
        AtomicInteger lakeLookupCount = new AtomicInteger();
        TestingPaimonLakeStoragePlugin.setLookupFunction(
                (key, context) -> {
                    lakeLookupCount.incrementAndGet();
                    assertThat(context.partitionSpec().toPartitionSpec())
                            .isEqualTo(partitionSpec(MISSING_PARTITION));
                    return row(3, "lake", MISSING_PARTITION);
                });
        openLookupFunction(Duration.ofSeconds(5));

        assertThat(lookup(3, MISSING_PARTITION))
                .singleElement()
                .extracting(RowData::toString)
                .isEqualTo("+I(3,lake,1900)");
        assertThat(lakeLookupCount).hasValue(1);
    }

    @Test
    void testEmptyLakeLookupResult() throws Exception {
        openLookupFunction(Duration.ofSeconds(5));

        assertThat(lookup(4, MISSING_PARTITION)).isEmpty();
    }

    @Test
    void testLakeLookupFailureIsPropagated() {
        TestingPaimonLakeStoragePlugin.setLookupFunction(
                (key, context) -> {
                    throw new IOException("lake lookup failure");
                });
        openLookupFunction(Duration.ofSeconds(5));

        assertThatThrownBy(() -> lookup(5, MISSING_PARTITION))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("lake lookup failure");
    }

    @Test
    void testLakeLookupTimeoutIsPropagated() throws Exception {
        CountDownLatch lookupStarted = new CountDownLatch(1);
        CountDownLatch releaseLookup = new CountDownLatch(1);
        TestingPaimonLakeStoragePlugin.setLookupFunction(
                (key, context) -> {
                    lookupStarted.countDown();
                    releaseLookup.await();
                    return row(6, "lake", MISSING_PARTITION);
                });
        openLookupFunction(Duration.ofMillis(100));

        CompletableFuture<Collection<RowData>> future =
                lookupFunction.asyncLookup(lookupKey(6, MISSING_PARTITION));
        assertThat(lookupStarted.await(10, TimeUnit.SECONDS)).isTrue();
        try {
            assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseInstanceOf(TimeoutException.class);
        } finally {
            releaseLookup.countDown();
        }
    }

    private void openLookupFunction(Duration timeout) {
        RowType flinkRowType = FlinkConversions.toFlinkRowType(TABLE_SCHEMA.getRowType());
        int[] primaryKeyIndexes = TABLE_SCHEMA.getPrimaryKeyIndexes();
        lookupFunction =
                new HybridLakeAsyncLookupFunction(
                        clientConf,
                        TABLE_PATH,
                        flinkRowType,
                        primaryKeyIndexes,
                        createPrimaryKeyLookupNormalizer(primaryKeyIndexes, flinkRowType),
                        null,
                        Collections.singletonMap(
                                ConfigOptions.TABLE_DATALAKE_FORMAT.key(),
                                DataLakeFormat.PAIMON.toString()),
                        timeout,
                        1,
                        2);
        lookupFunction.open(null);
    }

    private Collection<RowData> lookup(int id, String partition) throws Exception {
        return lookupFunction.asyncLookup(lookupKey(id, partition)).get(10, TimeUnit.SECONDS);
    }

    private static void writeRow(InternalRow row) throws Exception {
        try (Table table = conn.getTable(TABLE_PATH)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row);
            writer.flush();
        }
    }

    private static GenericRowData lookupKey(int id, String partition) {
        return GenericRowData.of(id, StringData.fromString(partition));
    }

    private static PartitionSpec partitionSpec(String partition) {
        return new PartitionSpec(Collections.singletonMap("date", partition));
    }
}
