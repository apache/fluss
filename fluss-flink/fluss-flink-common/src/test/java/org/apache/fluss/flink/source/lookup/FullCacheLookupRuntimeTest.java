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
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests snapshot bootstrap, CDC updates, failure publication and cache lifecycle. */
class FullCacheLookupRuntimeTest extends FlinkTestBase {

    @TempDir private Path tempDir;

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testSnapshotAndChangelog(KvFormat format) throws Exception {
        TablePath path = TablePath.of(DEFAULT_DB, "full-cache-cdc-" + format);
        createTable(
                path,
                TableDescriptor.builder()
                        .schema(DEFAULT_PK_TABLE_SCHEMA)
                        .distributedBy(3, "id")
                        .property(ConfigOptions.TABLE_KV_FORMAT, format)
                        .build());
        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int id = 0; id < 20; id++) {
                writer.upsert(row(id, "snapshot-" + id));
            }
            writer.flush();
            try (FullCacheLookupRuntime runtime = runtime(path, 3)) {
                TableInfo info = runtime.open(0, 1, new UnregisteredMetricsGroup());
                for (int id = 0; id < 20; id++) {
                    assertValue(runtime, info, id, "snapshot-" + id);
                }
                // Retain returned bytes across later writes to detect accidental serializer reuse.
                List<byte[]> original = runtime.lookup(key(info, 1), false);
                writer.upsert(row(1, "updated"));
                writer.delete(row(2, "snapshot-2"));
                writer.upsert(row(20, "inserted"));
                writer.flush();
                retry(
                        Duration.ofSeconds(30),
                        () -> {
                            assertValue(runtime, info, 1, "updated");
                            assertValue(runtime, info, 20, "inserted");
                            assertThat(runtime.lookup(key(info, 2), false)).isEmpty();
                        });
                assertThat(decode(info, original.get(0)).getString(1).toString())
                        .isEqualTo("snapshot-1");
            }
        }
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }

    @Test
    void testEmptySnapshotAndClose() throws Exception {
        TablePath path = TablePath.of(DEFAULT_DB, "full-cache-empty");
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(createTable(path, DEFAULT_PK_TABLE_DESCRIPTOR));
        FullCacheLookupRuntime runtime = runtime(path, DEFAULT_BUCKET_NUM);
        try {
            TableInfo info = runtime.open(0, 1, new UnregisteredMetricsGroup());
            assertThat(runtime.lookup(key(info, 1), false)).isEmpty();
        } finally {
            runtime.close();
        }
        runtime.close();
        assertThatThrownBy(runtime::awaitReady).hasMessageContaining("not ready");
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }

    @Test
    void testFailedBatchCannotBeRead() throws Exception {
        TablePath path = TablePath.of(DEFAULT_DB, "full-cache-failure");
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(createTable(path, DEFAULT_PK_TABLE_DESCRIPTOR));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch partialWrite = new CountDownLatch(1);
        CountDownLatch failBatch = new CountDownLatch(1);
        try (FullCacheLookupRuntime runtime = runtime(path, DEFAULT_BUCKET_NUM)) {
            TableInfo info = runtime.open(0, 1, new UnregisteredMetricsGroup());
            runtime.awaitReady();
            IllegalStateException failure = new IllegalStateException("injected batch failure");
            ScanRecord broken = mock(ScanRecord.class);
            when(broken.getChangeType())
                    .thenAnswer(
                            invocation -> {
                                partialWrite.countDown();
                                failBatch.await();
                                throw failure;
                            });
            ScanRecords records =
                    new ScanRecords(
                            Collections.singletonMap(
                                    new TableBucket(info.getTableId(), 0),
                                    Arrays.asList(new ScanRecord(row(1, "partial")), broken)));
            Future<?> update =
                    executor.submit(
                            () -> {
                                runtime.applyLogRecords(records);
                                return null;
                            });
            partialWrite.await();
            CountDownLatch readerStarted = new CountDownLatch(1);
            Future<?> read =
                    executor.submit(
                            () -> {
                                readerStarted.countDown();
                                return runtime.lookup(key(info, 1), false);
                            });
            readerStarted.await();
            failBatch.countDown();
            assertThatThrownBy(update::get).hasCause(failure);
            assertThatThrownBy(read::get).hasRootCause(failure);
            assertThatThrownBy(() -> runtime.lookup(key(info, 1), false)).hasCause(failure);
        } finally {
            failBatch.countDown();
            executor.shutdownNow();
        }
    }

    private FullCacheLookupRuntime runtime(TablePath path, int buckets) {
        Configuration config = new Configuration(clientConf);
        config.set(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR, tempDir.toString());
        return new FullCacheLookupRuntime(config, path, buckets);
    }

    private static byte[] key(TableInfo info, int id) {
        return KeyEncoder.ofPrimaryKeyEncoder(
                        info.getRowType(),
                        info.getPhysicalPrimaryKeys(),
                        info.getTableConfig(),
                        info.isDefaultBucketKey())
                .encodeKey(row(id, null));
    }

    private static InternalRow decode(TableInfo info, byte[] value) {
        return RowDecoder.create(
                        info.getTableConfig().getKvFormat(),
                        info.getRowType().getChildren().toArray(new DataType[0]))
                .decode(value);
    }

    private static void assertValue(
            FullCacheLookupRuntime runtime, TableInfo info, int id, String expected)
            throws Exception {
        List<byte[]> values = runtime.lookup(key(info, id), false);
        assertThat(values).hasSize(1);
        InternalRow row = decode(info, values.get(0));
        assertThat(row.getInt(0)).isEqualTo(id);
        assertThat(row.getString(1).toString()).isEqualTo(expected);
    }
}
