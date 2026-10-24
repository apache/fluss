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

package org.apache.fluss.server.index;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Source failover while an asynchronous index target is unavailable. */
class IndexPushOrderingITCase {

    private static final Logger LOG = LoggerFactory.getLogger(IndexPushOrderingITCase.class);

    private static final int TABLET_SERVER_COUNT = 6;
    private static final int REPLICATION_FACTOR = 3;
    private static final int INDEX_BUCKET_COUNT = 20;
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final String INDEX_NAME = "idx_b";

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(TABLET_SERVER_COUNT)
                    .setClusterConf(configuration())
                    .build();

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, REPLICATION_FACTOR);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.INDEX_REPLICATION_RETRY_BACKOFF, Duration.ofMillis(10));
        conf.set(ConfigOptions.INDEX_REPLICATION_SOURCE_MAX_BYTES, MemorySize.parse("64b"));
        // Stopping the target bucket's replicas may also take down a source follower, so the source
        // high watermark only advances once that follower drops out of the ISR.
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(2));
        return conf;
    }

    @Test
    void replaysBacklogAfterSourceFailoverAndTargetRecovery() throws Exception {
        Set<Integer> stoppedServers = new LinkedHashSet<>();
        Throwable failure = null;
        try {
            Fixture fixture = createFixture();
            String oldValue =
                    valueForBucket(
                            fixture.indexSpec, "ordering-old", fixture.targetBucket.getBucket());
            String newValue =
                    valueForBucket(
                            fixture.indexSpec, "ordering-new", fixture.targetBucket.getBucket());

            put(fixture, fixture.sourceLeader, 1, oldValue);
            put(fixture, fixture.sourceLeader, 2, oldValue);
            waitForIndexEntry(fixture, oldValue, 1, true);
            waitForIndexEntry(fixture, oldValue, 2, true);

            Replica source = CLUSTER.waitAndGetLeaderReplica(fixture.sourceBucket);
            waitUntil(
                    () -> source.getAllIndexPushedOffset() == source.getLocalLogEndOffset(),
                    TIMEOUT,
                    "wait for baseline index progress");
            long baseline = source.getAllIndexPushedOffset();
            CompletedSnapshot snapshot = CLUSTER.triggerAndWaitSnapshot(fixture.sourceBucket);
            assertThat(snapshot.getIndexPushedOffset()).isEqualTo(baseline);

            for (int server : fixture.targetReplicas) {
                stop(server, stoppedServers);
            }

            put(fixture, fixture.sourceLeader, 1, newValue);
            delete(fixture, fixture.sourceLeader, 2);
            put(fixture, fixture.sourceLeader, 3, newValue);
            waitForSourceCommit(source);
            long oldLeaderEnd = source.getLocalLogEndOffset();
            assertThat(source.getAllIndexPushedOffset()).isEqualTo(baseline);
            assertThat(oldLeaderEnd).isGreaterThan(baseline);

            stop(fixture.sourceLeader, stoppedServers);
            int newSourceLeader = waitForNewLeader(fixture.sourceBucket, fixture.sourceLeader);
            Replica recoveredSource = CLUSTER.waitAndGetLeaderReplica(fixture.sourceBucket);
            assertThat(recoveredSource.getAllIndexPushedOffset()).isEqualTo(baseline);

            put(fixture, newSourceLeader, 4, newValue);
            waitForSourceCommit(recoveredSource);
            long recoveredEnd = recoveredSource.getLocalLogEndOffset();
            assertThat(recoveredEnd).isGreaterThan(oldLeaderEnd);
            assertThat(recoveredSource.getAllIndexPushedOffset()).isEqualTo(baseline);

            for (int server : new ArrayList<>(fixture.targetReplicas)) {
                start(server, stoppedServers);
            }
            CLUSTER.waitAndGetLeaderReplica(fixture.targetBucket);
            waitUntil(
                    () -> recoveredSource.getAllIndexPushedOffset() == recoveredEnd,
                    TIMEOUT,
                    "wait for replayed index backlog");

            waitForIndexEntry(fixture, oldValue, 1, false);
            waitForIndexEntry(fixture, oldValue, 2, false);
            waitForIndexEntry(fixture, newValue, 1, true);
            waitForIndexEntry(fixture, newValue, 3, true);
            waitForIndexEntry(fixture, newValue, 4, true);
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            List<Throwable> cleanupFailures = new ArrayList<>();
            for (int server : new LinkedHashSet<>(stoppedServers)) {
                try {
                    start(server, stoppedServers);
                } catch (Throwable cleanupFailure) {
                    cleanupFailures.add(cleanupFailure);
                }
            }
            try {
                CLUSTER.assertHasTabletServerNumber(TABLET_SERVER_COUNT);
            } catch (Throwable cleanupFailure) {
                cleanupFailures.add(cleanupFailure);
            }
            if (!cleanupFailures.isEmpty()) {
                AssertionError cleanup = new AssertionError("failed to restore test cluster");
                cleanupFailures.forEach(cleanup::addSuppressed);
                if (failure != null) {
                    failure.addSuppressed(cleanup);
                } else {
                    throw cleanup;
                }
            }
        }
    }

    private static Fixture createFixture() throws Exception {
        TablePath mainPath = TablePath.of("index_ordering", "main_" + System.nanoTime());
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                IndexVisibility.ASYNC,
                                INDEX_BUCKET_COUNT)
                        .build();
        long mainTableId =
                createTable(
                        CLUSTER,
                        mainPath,
                        TableDescriptor.builder().schema(schema).distributedBy(1, "a").build());
        long indexTableId =
                CLUSTER.getZooKeeperClient()
                        .getTable(
                                TablePath.of(
                                        mainPath.getDatabaseName(),
                                        IndexTableUtils.indexTableName(
                                                mainPath.getTableName(), INDEX_NAME)))
                        .orElseThrow(AssertionError::new)
                        .tableId;
        TableBucket sourceBucket = new TableBucket(mainTableId, 0);
        CLUSTER.waitUntilAllReplicaReady(sourceBucket);
        int sourceLeader = CLUSTER.waitAndGetLeader(sourceBucket);
        Replica sourceReplica = CLUSTER.waitAndGetLeaderReplica(sourceBucket);
        IndexSpec indexSpec =
                IndexSpecFactory.buildIndexSpecs(
                                sourceReplica.getTableInfo(),
                                sourceBucket,
                                CLUSTER.getTabletServerById(sourceLeader).getMetadataCache())
                        .get(0);

        TableAssignment sourceAssignment = assignment(mainTableId);
        Set<Integer> sourceReplicas =
                new LinkedHashSet<>(sourceAssignment.getBucketAssignment(0).getReplicas());
        assertThat(sourceReplicas).hasSize(REPLICATION_FACTOR).contains(sourceLeader);

        TableAssignment indexAssignment = assignment(indexTableId);
        for (int bucket = 0; bucket < INDEX_BUCKET_COUNT; bucket++) {
            TableBucket candidate = new TableBucket(indexTableId, bucket);
            List<Integer> targetReplicas =
                    indexAssignment.getBucketAssignment(bucket).getReplicas();
            // The scenario needs the source leader to survive stopping the target replicas so it
            // can keep writing and then be stopped on its own, plus one source follower outside the
            // target replicas so a new source leader can be elected afterwards. Full disjointness
            // would also satisfy this but the assigner does not guarantee such a bucket exists.
            if (targetReplicas.contains(sourceLeader)) {
                continue;
            }
            boolean electableFollowerSurvives =
                    sourceReplicas.stream()
                            .anyMatch(r -> r != sourceLeader && !targetReplicas.contains(r));
            if (!electableFollowerSurvives) {
                continue;
            }
            CLUSTER.waitUntilAllReplicaReady(candidate);
            assertThat(targetReplicas).hasSize(REPLICATION_FACTOR);
            List<Integer> overlap = new ArrayList<>(targetReplicas);
            overlap.retainAll(sourceReplicas);
            LOG.info(
                    "Fixture geometry: source replicas {} (leader {}), target bucket {} replicas {}, overlap {}",
                    sourceReplicas,
                    sourceLeader,
                    bucket,
                    targetReplicas,
                    overlap);
            return new Fixture(
                    mainTableId,
                    indexTableId,
                    sourceBucket,
                    sourceLeader,
                    candidate,
                    targetReplicas,
                    indexSpec);
        }
        throw new AssertionError(
                "no index bucket leaves the source leader and an electable follower running; "
                        + "source replicas "
                        + sourceReplicas
                        + ", source leader "
                        + sourceLeader);
    }

    private static TableAssignment assignment(long tableId) throws Exception {
        return CLUSTER.getZooKeeperClient()
                .getTableAssignment(tableId)
                .orElseThrow(() -> new AssertionError("missing assignment for table " + tableId));
    }

    private static void put(Fixture fixture, int server, int key, String value) throws Exception {
        write(fixture, server, key, value == null ? null : new Object[] {key, value});
    }

    private static void delete(Fixture fixture, int server, int key) throws Exception {
        write(fixture, server, key, null);
    }

    private static void write(Fixture fixture, int server, int key, Object[] value)
            throws Exception {
        TabletServerGateway gateway = CLUSTER.newTabletServerClientForNode(server);
        PutKvResponse response =
                gateway.putKv(
                                newPutKvRequest(
                                                fixture.mainTableId,
                                                0,
                                                1,
                                                value == null
                                                        ? genKvRecordBatch(
                                                                Collections.singletonList(
                                                                        Tuple2.of(
                                                                                new Object[] {key},
                                                                                null)))
                                                        : genKvRecordBatch(value))
                                        .setTimeoutMs((int) TIMEOUT.toMillis()))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(response.getBucketsRespsList())
                .allSatisfy(bucket -> assertThat(bucket.hasErrorCode()).isFalse());
    }

    private static void waitForSourceCommit(Replica replica) {
        waitUntil(
                () -> replica.getLogTablet().getHighWatermark() == replica.getLocalLogEndOffset(),
                TIMEOUT,
                "wait for source WAL commit");
    }

    private static int waitForNewLeader(TableBucket bucket, int previousLeader) {
        final int[] leader = {-1};
        waitUntil(
                () -> {
                    leader[0] = CLUSTER.waitAndGetLeader(bucket);
                    return leader[0] != previousLeader;
                },
                TIMEOUT,
                "wait for source leader failover");
        return leader[0];
    }

    private static void waitForIndexEntry(
            Fixture fixture, String value, int key, boolean expectedPresent) {
        IndexSpec.IndexEntry entry =
                fixture.indexSpec.encodeEntry(GenericRow.of(key, fromString(value)));
        byte[] indexKey = entry.key();
        TableBucket bucket = new TableBucket(fixture.indexTableId, entry.targetBucket());
        waitUntil(
                () -> {
                    Replica replica = CLUSTER.waitAndGetLeaderReplica(bucket);
                    boolean present =
                            replica.lookups(Collections.singletonList(indexKey)).get(0) != null;
                    return present == expectedPresent;
                },
                TIMEOUT,
                "wait for index entry " + value + '/' + key + " present=" + expectedPresent);
    }

    private static String valueForBucket(IndexSpec spec, String prefix, int bucket) {
        for (int suffix = 0; suffix < 100_000; suffix++) {
            String value = prefix + '-' + suffix;
            if (spec.encodeEntry(GenericRow.of(1, fromString(value))).targetBucket() == bucket) {
                return value;
            }
        }
        throw new AssertionError("unable to hash value to index bucket " + bucket);
    }

    private static void stop(int server, Set<Integer> stoppedServers) throws Exception {
        CLUSTER.stopTabletServer(server);
        assertThat(stoppedServers.add(server)).isTrue();
    }

    private static void start(int server, Set<Integer> stoppedServers) throws Exception {
        CLUSTER.startTabletServer(server);
        assertThat(stoppedServers.remove(server)).isTrue();
    }

    private static final class Fixture {
        private final long mainTableId;
        private final long indexTableId;
        private final TableBucket sourceBucket;
        private final int sourceLeader;
        private final TableBucket targetBucket;
        private final List<Integer> targetReplicas;
        private final IndexSpec indexSpec;

        private Fixture(
                long mainTableId,
                long indexTableId,
                TableBucket sourceBucket,
                int sourceLeader,
                TableBucket targetBucket,
                List<Integer> targetReplicas,
                IndexSpec indexSpec) {
            this.mainTableId = mainTableId;
            this.indexTableId = indexTableId;
            this.sourceBucket = sourceBucket;
            this.sourceLeader = sourceLeader;
            this.targetBucket = targetBucket;
            this.targetReplicas = new ArrayList<>(targetReplicas);
            this.indexSpec = indexSpec;
        }
    }
}
