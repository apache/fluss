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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Deduplication keys survive remote log expiration and snapshot-based failover. */
class DeduplicationRemoteRetentionITCase {
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
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofMillis(100));
        conf.set(ConfigOptions.LOG_RETENTION_CHECK_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        return conf;
    }

    @Test
    void testRemoteCleanupWaitsForSnapshotAndKeepsKeysAfterFailover() throws Exception {
        TablePath path = TablePath.of("dedup_remote_retention", "events");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, false).get();
            admin.createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("id", DataTypes.INT())
                                                    .column("payload", DataTypes.STRING())
                                                    .deduplicationKey("id")
                                                    .build())
                                    .distributedBy(1)
                                    .property(ConfigOptions.TABLE_LOG_TTL, Duration.ofSeconds(1))
                                    .property(
                                            ConfigOptions.TABLE_LOG_LOCAL_TTL, Duration.ofMillis(1))
                                    .build(),
                            false)
                    .get();
            TableBucket bucket = new TableBucket(admin.getTableInfo(path).get().getTableId(), 0);
            CLUSTER.waitUntilAllReplicaReady(bucket);
            int originalLeader = CLUSTER.waitAndGetLeader(bucket);
            Replica replica = CLUSTER.waitAndGetLeaderReplica(bucket);
            LogTablet log = replica.getLogTablet();
            RemoteLogManager remote =
                    CLUSTER.getTabletServerById(originalLeader)
                            .getReplicaManager()
                            .getRemoteLogManager();
            FsPath directory =
                    FlussPaths.remoteLogTabletDir(
                            FlussPaths.remoteLogDir(replica.getRemoteDataDir()),
                            PhysicalTablePath.of(path),
                            bucket);
            try (Table table = connection.getTable(path)) {
                AppendWriter writer = table.newAppend().createWriter();
                writer.append(GenericRow.of(1, BinaryString.fromString("first")))
                        .get(30, TimeUnit.SECONDS);
                log.roll(Optional.empty());
                waitUntil(
                        () ->
                                remote.remoteLogTablet(bucket)
                                                .currentManifest()
                                                .getHighestCopiedEndOffset()
                                        >= 1L,
                        Duration.ofSeconds(30),
                        "First segment must be committed remotely");
                RemoteLogSegment first =
                        committedManifest(remote, bucket).getRemoteLogSegmentList().get(0);

                CLOCK.advanceTime(Duration.ofSeconds(2));
                writer.append(GenericRow.of(2, BinaryString.fromString("second")))
                        .get(30, TimeUnit.SECONDS);
                log.roll(Optional.empty());
                // Publishing this segment also completes a TTL pass after the first expired.
                waitUntil(
                        () ->
                                remote.remoteLogTablet(bucket)
                                                .currentManifest()
                                                .getHighestCopiedEndOffset()
                                        >= 2L,
                        Duration.ofSeconds(30),
                        "Second remote segment must be committed");
                RemoteLogManifest beforeSnapshot = committedManifest(remote, bucket);
                assertThat(beforeSnapshot.getRemoteLogSegmentList())
                        .as(
                                "Expired segments must remain until the key-state snapshot is committed")
                        .contains(first)
                        .hasSize(2);
                List<FsPath> files =
                        beforeSnapshot.getRemoteLogSegmentList().stream()
                                .map(
                                        segment ->
                                                FlussPaths.remoteLogSegmentDir(
                                                        directory, segment.remoteLogSegmentId()))
                                .collect(Collectors.toList());
                for (FsPath file : files) {
                    assertThat(file.getFileSystem().exists(file)).isTrue();
                }
                log.deleteExpiredSegments();
                assertThat(log.localLogStartOffset()).isZero();
                assertThat(log.getMinRetainOffset()).isZero();

                CompletedSnapshot snapshot = CLUSTER.triggerAndWaitSnapshot(bucket);
                assertThat(snapshot.getLogOffset()).isEqualTo(2L);
                CLOCK.advanceTime(Duration.ofSeconds(2));
                waitUntil(
                        () -> {
                            if (!remote.remoteLogTablet(bucket)
                                            .currentManifest()
                                            .getRemoteLogSegmentList()
                                            .isEmpty()
                                    || !committedManifest(remote, bucket)
                                            .getRemoteLogSegmentList()
                                            .isEmpty()) {
                                return false;
                            }
                            for (FsPath file : files) {
                                if (file.getFileSystem().exists(file)) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        Duration.ofSeconds(30),
                        "Snapshot-covered remote files must expire");
                waitUntil(
                        () -> {
                            for (TabletServer server : CLUSTER.getTabletServers()) {
                                LogTablet local =
                                        server.getReplicaManager()
                                                .getReplicaOrException(bucket)
                                                .getLogTablet();
                                local.deleteExpiredSegments();
                                if (local.localLogStartOffset() != 2L) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        Duration.ofSeconds(30),
                        "Every replica must discard the original WAL prefix");

                CLUSTER.stopTabletServer(originalLeader);
                try (Connection fresh =
                                ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                        Table recovered = fresh.getTable(path)) {
                    assertThat(CLUSTER.waitAndGetLeader(bucket)).isNotEqualTo(originalLeader);
                    LogTablet successor = CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet();
                    long startOffset = successor.localLogEndOffset();
                    assertThat(startOffset).isEqualTo(2L);
                    AppendWriter next = recovered.newAppend().createWriter();
                    next.append(GenericRow.of(1, BinaryString.fromString("replacement")))
                            .get(30, TimeUnit.SECONDS);
                    next.append(GenericRow.of(2, BinaryString.fromString("replacement")))
                            .get(30, TimeUnit.SECONDS);
                    next.append(GenericRow.of(3, BinaryString.fromString("new")))
                            .get(30, TimeUnit.SECONDS);
                    long endOffset = successor.localLogEndOffset();
                    assertThat(endOffset).isEqualTo(startOffset + 3L);
                    List<String> rows = new ArrayList<>();
                    try (LogScanner scanner = recovered.newScan().createLogScanner()) {
                        scanner.subscribe(0, startOffset);
                        waitUntil(
                                () -> {
                                    ScanRecords records = scanner.poll(Duration.ofMillis(100));
                                    records.records(bucket)
                                            .forEach(
                                                    record -> {
                                                        InternalRow row = record.getRow();
                                                        rows.add(
                                                                row.getInt(0)
                                                                        + ":"
                                                                        + row.getString(1));
                                                    });
                                    Long progress = records.consumedUpToOffset(bucket);
                                    return progress != null && progress >= endOffset;
                                },
                                Duration.ofSeconds(30),
                                "Read duplicate progress and the new row");
                    }
                    assertThat(rows).containsExactly("3:new");
                } finally {
                    CLUSTER.startTabletServer(originalLeader);
                }
            }
        }
    }

    private RemoteLogManifest committedManifest(RemoteLogManager remote, TableBucket bucket)
            throws Exception {
        return remote.getRemoteLogStorage()
                .readRemoteLogManifestSnapshot(
                        CLUSTER.getZooKeeperClient()
                                .getRemoteLogManifestHandle(bucket)
                                .get()
                                .getRemoteLogManifestPath());
    }
}
