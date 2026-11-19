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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.ManualClock;

import io.github.fluss_contrib.rocksdb.RocksIterator;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Deduplication TTL preserves first-write age through snapshots and WAL recovery. */
class DeduplicationTtlITCase {
    private static final ManualClock CLOCK = new ManualClock(System.currentTimeMillis());

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(configuration())
                    .setClock(CLOCK)
                    .build();

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofDays(1));
        return conf;
    }

    @ParameterizedTest
    @EnumSource(LogFormat.class)
    void testKeyExpiryDoesNotRefreshOnDuplicatesOrFailover(LogFormat format) throws Exception {
        TablePath path =
                TablePath.of("dedup_ttl", "events_" + format.name().toLowerCase(Locale.ROOT));
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("id", DataTypes.STRING())
                                                    .column("payload", DataTypes.STRING())
                                                    .deduplicationKey("id")
                                                    .build())
                                    .distributedBy(1)
                                    .property(ConfigOptions.TABLE_LOG_FORMAT, format)
                                    .property("table.deduplication.ttl", "1 h")
                                    .build(),
                            false)
                    .get();
            TableBucket bucket = new TableBucket(admin.getTableInfo(path).get().getTableId(), 0);
            CLUSTER.waitUntilAllReplicaReady(bucket);
            try (Table table = connection.getTable(path)) {
                AppendWriter writer = table.newAppend().createWriter();
                long commitTimestamp = CLOCK.milliseconds();
                writer.append(row("aa-origin", "seed")).get(30, TimeUnit.SECONDS);
                CLOCK.advanceTime(Duration.ofMinutes(-10));
                try {
                    writer.append(row("zz-key-0001", "first")).get(30, TimeUnit.SECONDS);
                } finally {
                    CLOCK.advanceTime(Duration.ofMinutes(10));
                }
                CLOCK.advanceTime(Duration.ofMinutes(30));
                writer.append(row("zz-key-0001", "duplicate")).get(30, TimeUnit.SECONDS);
                CLUSTER.triggerAndWaitSnapshot(path);
                KvTablet tablet = CLUSTER.waitAndGetLeaderReplica(bucket).getKvTablet();
                RocksDBKv state = tablet.getRocksDBKv();
                List<Long> markerTimestamps = new ArrayList<>();
                try (RocksIterator keys =
                        state.getDb().newIterator(state.getDefaultColumnFamilyHandle())) {
                    for (keys.seekToFirst(); keys.isValid(); keys.next()) {
                        markerTimestamps.add(
                                tablet.getKvValueLayout()
                                        .readValueTag(MemorySegment.wrap(keys.value())));
                    }
                    keys.status();
                }
                assertThat(markerTimestamps).containsExactly(commitTimestamp, commitTimestamp);
                writer.append(row("zz-key-0002", "wal")).get(30, TimeUnit.SECONDS);

                CLOCK.advanceTime(Duration.ofMinutes(40));
                int oldLeader = CLUSTER.waitAndGetLeader(bucket);
                CLUSTER.stopTabletServer(oldLeader);
                try (Connection recoveredConnection =
                                ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                        Table recovered = recoveredConnection.getTable(path)) {
                    CLUSTER.waitAndGetLeaderReplica(bucket);
                    AppendWriter next = recovered.newAppend().createWriter();
                    compactAfterFreshKeys(next, path, bucket, 0);
                    next.append(row("zz-key-0001", "first-again")).get(30, TimeUnit.SECONDS);
                    next.append(row("zz-key-0002", "duplicate-after-failover"))
                            .get(30, TimeUnit.SECONDS);

                    CLOCK.advanceTime(Duration.ofMinutes(30));
                    compactAfterFreshKeys(next, path, bucket, 1001);
                    next.append(row("zz-key-0001", "duplicate-after-compaction"))
                            .get(30, TimeUnit.SECONDS);
                    next.append(row("zz-key-0002", "wal-again")).get(30, TimeUnit.SECONDS);

                    long end =
                            CLUSTER.waitAndGetLeaderReplica(bucket)
                                    .getLogTablet()
                                    .localLogEndOffset();
                    List<String> payloads = new ArrayList<>();
                    try (LogScanner scanner = recovered.newScan().createLogScanner()) {
                        scanner.subscribe(0, 0);
                        waitUntil(
                                () -> {
                                    ScanRecords records = scanner.poll(Duration.ofMillis(100));
                                    records.records(bucket)
                                            .forEach(
                                                    record -> {
                                                        if (record.getRow()
                                                                .getString(0)
                                                                .toString()
                                                                .startsWith("zz-")) {
                                                            payloads.add(
                                                                    record.getRow()
                                                                            .getString(1)
                                                                            .toString());
                                                        }
                                                    });
                                    Long progress = records.consumedUpToOffset(bucket);
                                    return progress != null && progress >= end;
                                },
                                Duration.ofSeconds(30),
                                "Read accepted records across key expiration");
                    }
                    assertThat(payloads)
                            .containsExactly("first", "wal", "first-again", "wal-again");
                } finally {
                    CLUSTER.startTabletServer(oldLeader);
                }
            }
        }
    }

    private static void compactAfterFreshKeys(
            AppendWriter writer, TablePath path, TableBucket bucket, int firstKey)
            throws Exception {
        // Cross the native filter's 1000-entry clock refresh before visiting the target keys.
        for (int i = firstKey; i < firstKey + 1001; i++) {
            writer.append(row(String.format(Locale.ROOT, "aa-key-%04d", i), "fresh"));
        }
        writer.flush();
        CLUSTER.triggerAndWaitSnapshot(path);
        CLUSTER.waitAndGetLeaderReplica(bucket).getKvTablet().getRocksDBKv().getDb().compactRange();
    }
}
