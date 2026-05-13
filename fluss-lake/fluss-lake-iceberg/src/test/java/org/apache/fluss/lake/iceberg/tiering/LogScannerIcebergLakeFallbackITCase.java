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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.source.IcebergLakeSource;
import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for log data tiered to Iceberg then read via {@link LogScanner} with a real
 * {@link IcebergLakeSource}, after local log truncation forces lake fallback for historical
 * offsets.
 */
class LogScannerIcebergLakeFallbackITCase extends FlinkIcebergTieringTestBase {

    private static final String DEFAULT_DB = "fluss";

    private static final Schema LOG_SCHEMA =
            Schema.newBuilder()
                    .column("f_int", DataTypes.INT())
                    .column("f_str", DataTypes.STRING())
                    .build();

    @Test
    void testReadTieredLogFromIcebergAfterLocalTruncate() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "log_scanner_iceberg_lake_fallback");
        long tableId = createLogTable(tablePath, 1, false, LOG_SCHEMA);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        List<InternalRow> batchA =
                Arrays.asList(row(1, "a0"), row(2, "a1"), row(3, "a2"), row(4, "a3"), row(5, "a4"));
        writeRows(tablePath, batchA, true);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(tableBucket, batchA.size());
            checkDataInIcebergAppendOnlyTable(tablePath, batchA, 0L);
        } finally {
            jobClient.cancel().get();
        }

        long lakeLogEndOffset = getLeaderReplica(tableBucket).getLakeLogEndOffset();
        assertThat(lakeLogEndOffset).isEqualTo(batchA.size());

        Replica leader = getLeaderReplica(tableBucket);
        leader.truncateFullyAndStartAt(lakeLogEndOffset);

        Configuration icebergConf = Configuration.fromMap(getIcebergCatalogConf());
        IcebergLakeSource icebergLakeSource = new IcebergLakeSource(icebergConf, tablePath);
        @SuppressWarnings("unchecked")
        LakeSource<LakeSplit> lakeSource =
                (LakeSource<LakeSplit>) (LakeSource<?>) icebergLakeSource;

        List<InternalRow> polledResult = new ArrayList<>();
        try (Table table = conn.getTable(tablePath);
                LogScanner lakeScanner = table.newScan().createLogScanner(lakeSource)) {
            lakeScanner.subscribeFromBeginning(0);
            pollScanRecordsUntilCount(
                    lakeScanner, batchA.size(), polledResult, Duration.ofMinutes(2));
            assertThat(polledResult).containsExactlyInAnyOrderElementsOf(batchA);
        }
    }

    /**
     * Polls until {@code targetNewCount} additional rows have been collected (relative to the list
     * size at invocation).
     */
    private static void pollScanRecordsUntilCount(
            LogScanner logScanner,
            int targetNewCount,
            List<InternalRow> sink,
            Duration overallTimeout)
            throws Exception {
        int startSize = sink.size();
        long deadlineNanos = System.nanoTime() + overallTimeout.toNanos();
        while (sink.size() - startSize < targetNewCount) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError(
                        String.format(
                                "Timed out after %s waiting for %d scan records (got %d)",
                                overallTimeout, targetNewCount, sink.size() - startSize));
            }
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(5));
            for (ScanRecord scanRecord : scanRecords) {
                assertThat(scanRecord.getChangeType())
                        .isIn(ChangeType.APPEND_ONLY, ChangeType.INSERT);
                InternalRow row = scanRecord.getRow();
                sink.add(row);
            }
        }
    }
}
