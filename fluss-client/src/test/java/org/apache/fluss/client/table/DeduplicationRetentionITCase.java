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
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Permanent key state survives expiration of the accepted log record. */
class DeduplicationRetentionITCase {
    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(configuration())
                    .build();

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO);
        return conf;
    }

    @Test
    void testLogCleanupWaitsForSnapshotAndKeepsKeysAfterFailover() throws Exception {
        TablePath path = TablePath.of("dedup_retention", "events");
        try (Connection conn = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = conn.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, false).get();
            admin.createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("id", DataTypes.INT())
                                                    .deduplicationKey("id")
                                                    .build())
                                    .distributedBy(1)
                                    .property(ConfigOptions.TABLE_LOG_TTL, Duration.ofMillis(1))
                                    .build(),
                            false)
                    .get();
            TableBucket bucket =
                    new TableBucket(admin.getTableInfo(path).get().getTableId(), null, 0);
            try (Table table = conn.getTable(path)) {
                AppendWriter writer = table.newAppend().createWriter();
                writer.append(GenericRow.of(1)).get(30, TimeUnit.SECONDS);
                LogTablet log = CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet();
                log.roll(Optional.empty());
                log.deleteExpiredSegments();
                assertThat(log.logStartOffset()).isZero();
                CLUSTER.triggerAndWaitSnapshot(path);
                waitUntil(
                        () -> {
                            log.deleteExpiredSegments();
                            return log.logStartOffset() > 0;
                        },
                        Duration.ofSeconds(30),
                        "Snapshot-covered log must expire");
                int leader = CLUSTER.getZooKeeperClient().getLeaderAndIsr(bucket).get().leader();
                CLUSTER.stopTabletServer(leader);
                try (Connection fresh =
                                ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                        Table recovered = fresh.getTable(path)) {
                    LogTablet successor = CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet();
                    long startOffset = successor.localLogEndOffset();
                    AppendWriter next = recovered.newAppend().createWriter();
                    next.append(GenericRow.of(1)).get(30, TimeUnit.SECONDS);
                    next.append(GenericRow.of(2)).get(30, TimeUnit.SECONDS);
                    long endOffset = successor.localLogEndOffset();
                    List<InternalRow> rows = new ArrayList<>();
                    try (LogScanner scanner = recovered.newScan().createLogScanner()) {
                        scanner.subscribe(0, startOffset);
                        waitUntil(
                                () -> {
                                    ScanRecords records = scanner.poll(Duration.ofMillis(100));
                                    records.records(bucket)
                                            .forEach(record -> rows.add(record.getRow()));
                                    Long progress = records.consumedUpToOffset(bucket);
                                    return progress != null && progress >= endOffset;
                                },
                                Duration.ofSeconds(30),
                                "Read appends after recovery");
                    }
                    assertThat(rows).hasSize(1);
                    assertThat(rows.get(0).getInt(0)).isEqualTo(2);
                } finally {
                    CLUSTER.startTabletServer(leader);
                }
            }
        }
    }
}
