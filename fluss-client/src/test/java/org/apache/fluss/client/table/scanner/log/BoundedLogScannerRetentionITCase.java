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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests bounded subscriptions after retention advances the server's earliest offset. */
@Execution(ExecutionMode.SAME_THREAD)
class BoundedLogScannerRetentionITCase {

    private static final ManualClock CLOCK = new ManualClock(System.currentTimeMillis());

    private static final int EXPIRED_RECORDS = 10;
    private static final long STOPPING_OFFSET = 5L;

    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .setClock(CLOCK)
                    .build();

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEarliestBeyondStoppingOffset(boolean retainRecords) throws Exception {
        TablePath tablePath = TablePath.of("bounded_retention", "logs_" + retainRecords);

        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(
                            tablePath,
                            TableDescriptor.builder()
                                    .schema(DATA1_SCHEMA)
                                    .distributedBy(1)
                                    .logFormat(LogFormat.ARROW)
                                    .property(ConfigOptions.TABLE_LOG_TTL, Duration.ofHours(1))
                                    .build(),
                            false)
                    .get();

            long tableId = admin.getTableInfo(tablePath).get().getTableId();
            TableBucket bucket = new TableBucket(tableId, 0);
            LogTablet logTablet = CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet();

            try (Table table = connection.getTable(tablePath)) {
                AppendWriter writer = table.newAppend().createWriter();
                appendExpiredRecords(writer);
                retry(
                        Duration.ofSeconds(10),
                        () ->
                                assertThat(logTablet.getHighWatermark())
                                        .isEqualTo((long) EXPIRED_RECORDS));

                // Move the written records into a closed segment so that retention
                // can remove the whole segment.
                logTablet.roll(Optional.empty());
                CLOCK.advanceTime(Duration.ofHours(2));
                logTablet.deleteExpiredSegments();

                assertThat(logTablet.logStartOffset()).isEqualTo((long) EXPIRED_RECORDS);
                assertThat(logTablet.localLogEndOffset()).isEqualTo((long) EXPIRED_RECORDS);

                if (retainRecords) {
                    writer.append(row(EXPIRED_RECORDS, "retained"));
                    writer.flush();
                    retry(
                            Duration.ofSeconds(10),
                            () ->
                                    assertThat(logTablet.getHighWatermark())
                                            .isEqualTo(EXPIRED_RECORDS + 1L));
                }

                assertThat(logTablet.logStartOffset()).isEqualTo((long) EXPIRED_RECORDS);

                // Exercise both scanner APIs against the same retained log state.
                assertBoundedScanFinishes(table, bucket, false);
                assertBoundedScanFinishes(table, bucket, true);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExplicitStartingOffsetBeforeLogStartFails(boolean useArrowPoll) throws Exception {
        TablePath tablePath =
                TablePath.of("bounded_retention", "explicit_offset_out_of_range_" + useArrowPoll);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(
                            tablePath,
                            TableDescriptor.builder()
                                    .schema(DATA1_SCHEMA)
                                    .distributedBy(1)
                                    .logFormat(LogFormat.ARROW)
                                    .property(ConfigOptions.TABLE_LOG_TTL, Duration.ofHours(1))
                                    .build(),
                            false)
                    .get();

            long tableId = admin.getTableInfo(tablePath).get().getTableId();
            TableBucket bucket = new TableBucket(tableId, 0);
            LogTablet logTablet = CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet();

            try (Table table = connection.getTable(tablePath)) {
                AppendWriter writer = table.newAppend().createWriter();
                appendExpiredRecords(writer);
                retry(
                        Duration.ofSeconds(10),
                        () ->
                                assertThat(logTablet.getHighWatermark())
                                        .isEqualTo((long) EXPIRED_RECORDS));

                // Move offsets [0, EXPIRED_RECORDS) into a closed segment and expire them.
                logTablet.roll(Optional.empty());
                CLOCK.advanceTime(Duration.ofHours(2));
                logTablet.deleteExpiredSegments();

                assertThat(logTablet.logStartOffset()).isEqualTo((long) EXPIRED_RECORDS);
                assertThat(logTablet.localLogEndOffset()).isEqualTo((long) EXPIRED_RECORDS);

                try (LogScannerImpl scanner = (LogScannerImpl) table.newScan().createLogScanner()) {

                    // Unlike EARLIEST_OFFSET, an explicit historical offset must not
                    // be silently advanced to the current log start.
                    scanner.subscribeBounded(0, 0L, EXPIRED_RECORDS + 5L);
                    if (useArrowPoll) {
                        assertThatThrownBy(() -> scanner.pollRecordBatch(Duration.ofSeconds(1)))
                                .isInstanceOf(FetchException.class)
                                .hasMessageContaining("offset 0")
                                .hasMessageContaining("out of range");
                    } else {
                        assertThatThrownBy(() -> scanner.poll(Duration.ofSeconds(1)))
                                .isInstanceOf(FetchException.class)
                                .hasMessageContaining("offset 0")
                                .hasMessageContaining("out of range");
                    }
                }
            }
        }
    }

    private static void appendExpiredRecords(AppendWriter writer) {
        for (int i = 0; i < EXPIRED_RECORDS; i++) {
            writer.append(row(i, "expired"));
        }

        // flush() waits for all previously appended records to complete, avoiding
        // one synchronous round trip per record.
        writer.flush();
    }

    private static void assertBoundedScanFinishes(
            Table table, TableBucket bucket, boolean useArrowPoll) {
        try (LogScannerImpl scanner = (LogScannerImpl) table.newScan().createLogScanner()) {

            scanner.subscribeBounded(0, LogScanner.EARLIEST_OFFSET, STOPPING_OFFSET);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            boolean finished = false;
            while (!finished) {
                assertThat(System.nanoTime())
                        .as(
                                "Bounded scan must finish without waiting for "
                                        + "new records, useArrowPoll=%s",
                                useArrowPoll)
                        .isLessThan(deadline);

                finished = pollEmptyRange(scanner, useArrowPoll, bucket);
            }

            // Once the bucket is finished, subsequent non-blocking polls must
            // not report the same progress again.
            if (useArrowPoll) {
                try (ArrowScanRecords records = scanner.pollRecordBatch(Duration.ZERO)) {
                    assertThat(records.hasProgress()).isFalse();
                }
            } else {
                assertThat(scanner.poll(Duration.ZERO).hasProgress()).isFalse();
            }
        }
    }

    private static boolean pollEmptyRange(
            LogScannerImpl scanner, boolean useArrowPoll, TableBucket bucket) {
        if (useArrowPoll) {
            try (ArrowScanRecords records = scanner.pollRecordBatch(Duration.ofMillis(100))) {
                assertThat(records.count()).isZero();
                if (records.finishedBuckets().contains(bucket)) {
                    assertThat(records.finishedBuckets()).containsExactly(bucket);
                    assertThat(records.consumedUpToOffset(bucket)).isEqualTo(STOPPING_OFFSET);
                    return true;
                }
            }
        } else {
            ScanRecords records = scanner.poll(Duration.ofMillis(100));
            assertThat(records.count()).isZero();
            if (records.finishedBuckets().contains(bucket)) {
                assertThat(records.finishedBuckets()).containsExactly(bucket);
                assertThat(records.consumedUpToOffset(bucket)).isEqualTo(STOPPING_OFFSET);
                return true;
            }
        }
        return false;
    }

    private static Configuration clusterConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        // Exercise local retention deterministically, without waiting for
        // remote uploads.
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO);
        return conf;
    }
}
