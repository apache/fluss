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

package org.apache.fluss.server.coordinator.lease;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketSnapshot;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.lease.KvSnapshotTableLease;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvSnapshotLeaseManager}. */
public class KvSnapshotLeaseManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final long PARTITION_ID_1 = 19001L;
    private static final long PARTITION_ID_2 = 19002L;

    private static final TableBucket t0b0 = new TableBucket(DATA1_TABLE_ID_PK, 0);
    private static final TableBucket t0b1 = new TableBucket(DATA1_TABLE_ID_PK, 1);
    private static final TableBucket t1p0b0 =
            new TableBucket(PARTITION_TABLE_ID, PARTITION_ID_1, 0);
    private static final TableBucket t1p0b1 =
            new TableBucket(PARTITION_TABLE_ID, PARTITION_ID_1, 1);
    private static final TableBucket t1p1b0 =
            new TableBucket(PARTITION_TABLE_ID, PARTITION_ID_2, 0);

    protected static ZooKeeperClient zookeeperClient;

    private ManualClock manualClock;
    private ManuallyTriggeredScheduledExecutorService clearLeaseScheduler;
    private KvSnapshotLeaseManager kvSnapshotLeaseManager;

    private @TempDir Path tempDir;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void beforeEach() throws Exception {
        manualClock = new ManualClock(System.currentTimeMillis());
        clearLeaseScheduler = new ManuallyTriggeredScheduledExecutorService();
        kvSnapshotLeaseManager =
                new KvSnapshotLeaseManager(
                        Duration.ofDays(7).toMillis(),
                        zookeeperClient,
                        tempDir.toString(),
                        clearLeaseScheduler,
                        manualClock,
                        TestingMetricGroups.COORDINATOR_METRICS);
        kvSnapshotLeaseManager.start();
        initialZookeeper();
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    private static void initialZookeeper() throws Exception {
        List<TableBucket> tableBuckets = Arrays.asList(t0b0, t0b1, t1p0b0, t1p0b1, t1p1b0);
        for (TableBucket tb : tableBuckets) {
            zookeeperClient.registerTableBucketSnapshot(
                    tb, new BucketSnapshot(0L, 0L, "test-path"));
        }
    }

    @Test
    void testInitialize() throws Exception {
        assertThat(
                        snapshotLeaseNotExists(
                                Arrays.asList(
                                        new TableBucketSnapshot(t0b0, 0L),
                                        new TableBucketSnapshot(t0b1, 0L),
                                        new TableBucketSnapshot(t1p0b0, 0L),
                                        new TableBucketSnapshot(t1p0b1, 0L),
                                        new TableBucketSnapshot(t1p1b0, 0L))))
                .isTrue();
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(0);

        // test initialize from zookeeper when coordinator is started.
        KvSnapshotLeaseHandler kvSnapshotLeasehandle = new KvSnapshotLeaseHandler(1000L);
        acquire(kvSnapshotLeasehandle, new TableBucketSnapshot(t0b0, 0L));
        acquire(kvSnapshotLeasehandle, new TableBucketSnapshot(t0b1, 0L));
        acquire(kvSnapshotLeasehandle, new TableBucketSnapshot(t1p0b0, 0L));
        acquire(kvSnapshotLeasehandle, new TableBucketSnapshot(t1p0b1, 0L));
        acquire(kvSnapshotLeasehandle, new TableBucketSnapshot(t1p1b0, 0L));
        kvSnapshotLeaseManager.getMetadataManager().registerLease("lease1", kvSnapshotLeasehandle);

        kvSnapshotLeaseManager.start();

        assertThat(
                        snapshotLeaseExists(
                                Arrays.asList(
                                        new TableBucketSnapshot(t0b0, 0L),
                                        new TableBucketSnapshot(t0b1, 0L),
                                        new TableBucketSnapshot(t1p0b0, 0L),
                                        new TableBucketSnapshot(t1p0b1, 0L),
                                        new TableBucketSnapshot(t1p1b0, 0L))))
                .isTrue();
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(5);

        // check detail content.
        // Array sizes are dynamically determined by the maximum bucket id:
        // - DATA1_TABLE_ID_PK: bucket 0 and 1 → array size 2
        // - PARTITION_ID_1: bucket 0 and 1 → array size 2
        // - PARTITION_ID_2: bucket 0 only → array size 1
        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        tableIdToTableLease.put(
                DATA1_TABLE_ID_PK,
                new KvSnapshotTableLease(DATA1_TABLE_ID_PK, new Long[] {0L, 0L}));
        KvSnapshotTableLease leaseForPartitionTable = new KvSnapshotTableLease(PARTITION_TABLE_ID);
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_1, new Long[] {0L, 0L});
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_2, new Long[] {0L});
        tableIdToTableLease.put(PARTITION_TABLE_ID, leaseForPartitionTable);
        KvSnapshotLeaseHandler expectedLease =
                new KvSnapshotLeaseHandler(1000L, tableIdToTableLease);
        assertThat(kvSnapshotLeaseManager.getKvSnapshotLeaseData("lease1"))
                .isEqualTo(expectedLease);
        assertThat(kvSnapshotLeaseManager.getMetadataManager().getLease("lease1"))
                .hasValue(expectedLease);
    }

    @Test
    void testAcquireAndRelease() throws Exception {
        Map<Long, List<TableBucketSnapshot>> tableIdToRegisterBucket = initRegisterBuckets();
        acquire("lease1", tableIdToRegisterBucket);

        // first register snapshot to zk.
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b0, new BucketSnapshot(1L, 10L, "test-path"));
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b1, new BucketSnapshot(1L, 10L, "test-path"));

        tableIdToRegisterBucket = new HashMap<>();
        tableIdToRegisterBucket.put(
                PARTITION_TABLE_ID,
                Arrays.asList(
                        new TableBucketSnapshot(t1p0b0, 1L), new TableBucketSnapshot(t1p0b1, 1L)));
        acquire("lease2", tableIdToRegisterBucket);

        assertThat(
                        snapshotLeaseExists(
                                Arrays.asList(
                                        new TableBucketSnapshot(t0b0, 0L),
                                        new TableBucketSnapshot(t0b1, 0L),
                                        new TableBucketSnapshot(t1p0b0, 0),
                                        new TableBucketSnapshot(t1p0b1, 0L),
                                        new TableBucketSnapshot(t1p1b0, 0L),
                                        new TableBucketSnapshot(t1p0b0, 1L),
                                        new TableBucketSnapshot(t1p0b1, 1L))))
                .isTrue();
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(7);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(2);

        // update lease register.
        tableIdToRegisterBucket = new HashMap<>();
        zookeeperClient.registerTableBucketSnapshot(t0b0, new BucketSnapshot(1L, 10L, "test-path"));
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK, Collections.singletonList(new TableBucketSnapshot(t0b0, 1L)));
        acquire("lease1", tableIdToRegisterBucket);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(7);

        // new insert.
        tableIdToRegisterBucket = new HashMap<>();
        TableBucket newTableBucket = new TableBucket(DATA1_TABLE_ID_PK, 2);

        zookeeperClient.registerTableBucketSnapshot(
                newTableBucket, new BucketSnapshot(1L, 10L, "test-path"));
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Collections.singletonList(new TableBucketSnapshot(newTableBucket, 1L)));
        acquire("lease1", tableIdToRegisterBucket);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(8);

        // release
        release("lease1", Collections.singletonList(newTableBucket));
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(7);

        // release a non-exist bucket.
        release(
                "lease1",
                Collections.singletonList(new TableBucket(DATA1_TABLE_ID_PK, PARTITION_ID_1, 2)));
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(7);

        // check detail content for lease1.
        // - DATA1_TABLE_ID_PK: bucket 0,1 initially (size 2), then bucket 2 acquired and
        //   released → expanded to size 3: {1L, 0L, -1L}
        // - PARTITION_ID_1: bucket 0,1 → array size 2
        // - PARTITION_ID_2: bucket 0 only → array size 1
        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        tableIdToTableLease.put(
                DATA1_TABLE_ID_PK,
                new KvSnapshotTableLease(DATA1_TABLE_ID_PK, new Long[] {1L, 0L, -1L}));
        KvSnapshotTableLease leaseForPartitionTable = new KvSnapshotTableLease(PARTITION_TABLE_ID);
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_1, new Long[] {0L, 0L});
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_2, new Long[] {0L});
        tableIdToTableLease.put(PARTITION_TABLE_ID, leaseForPartitionTable);
        KvSnapshotLeaseHandler expectedLease =
                new KvSnapshotLeaseHandler(manualClock.milliseconds() + 1000L, tableIdToTableLease);
        assertThat(kvSnapshotLeaseManager.getKvSnapshotLeaseData("lease1"))
                .isEqualTo(expectedLease);
        assertThat(kvSnapshotLeaseManager.getMetadataManager().getLease("lease1"))
                .hasValue(expectedLease);

        // check detail content for lease2.
        // - PARTITION_ID_1: bucket 0,1 → array size 2
        tableIdToTableLease = new HashMap<>();
        leaseForPartitionTable = new KvSnapshotTableLease(PARTITION_TABLE_ID);
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_1, new Long[] {1L, 1L});
        tableIdToTableLease.put(PARTITION_TABLE_ID, leaseForPartitionTable);
        KvSnapshotLeaseHandler expectedLease2 =
                new KvSnapshotLeaseHandler(manualClock.milliseconds() + 1000L, tableIdToTableLease);
        assertThat(kvSnapshotLeaseManager.getKvSnapshotLeaseData("lease2"))
                .isEqualTo(expectedLease2);
        assertThat(kvSnapshotLeaseManager.getMetadataManager().getLease("lease2"))
                .hasValue(expectedLease2);
    }

    @Test
    void testDropLease() throws Exception {
        Map<Long, List<TableBucketSnapshot>> tableIdToRegisterBucket = initRegisterBuckets();
        acquire("lease1", tableIdToRegisterBucket);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(5);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(1);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease1")).isPresent();

        // release all will clear this lease.
        release("lease1", Arrays.asList(t0b0, t0b1, t1p0b0, t1p0b1, t1p1b0));
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(0);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(0);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease1")).isNotPresent();
    }

    @Test
    void testClear() throws Exception {
        Map<Long, List<TableBucketSnapshot>> tableIdToRegisterBucket = initRegisterBuckets();
        acquire("lease1", tableIdToRegisterBucket);

        // first register snapshot to zk.
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b0, new BucketSnapshot(1L, 10L, "test-path"));
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b1, new BucketSnapshot(1L, 10L, "test-path"));
        tableIdToRegisterBucket = new HashMap<>();
        tableIdToRegisterBucket.put(
                PARTITION_TABLE_ID,
                Arrays.asList(
                        new TableBucketSnapshot(t0b0, 0L), // same ref.
                        new TableBucketSnapshot(t1p0b0, 1L),
                        new TableBucketSnapshot(t1p0b1, 1L)));
        acquire("lease2", tableIdToRegisterBucket);

        assertThat(
                        snapshotLeaseExists(
                                Arrays.asList(
                                        new TableBucketSnapshot(t0b0, 0L),
                                        new TableBucketSnapshot(t0b1, 0L),
                                        new TableBucketSnapshot(t1p0b0, 0L),
                                        new TableBucketSnapshot(t1p0b1, 0L),
                                        new TableBucketSnapshot(t1p1b0, 0L),
                                        new TableBucketSnapshot(t1p0b0, 1L),
                                        new TableBucketSnapshot(t1p0b1, 1L))))
                .isTrue();
        assertThat(kvSnapshotLeaseManager.getRefCount(new TableBucketSnapshot(t0b0, 0L)))
                .isEqualTo(2);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(8);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(2);

        kvSnapshotLeaseManager.dropLease("lease1");
        assertThat(kvSnapshotLeaseManager.getRefCount(new TableBucketSnapshot(t0b0, 0L)))
                .isEqualTo(1);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(3);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(1);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease1")).isEmpty();

        kvSnapshotLeaseManager.dropLease("lease2");
        assertThat(kvSnapshotLeaseManager.getRefCount(new TableBucketSnapshot(t0b0, 0L)))
                .isEqualTo(0);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(0);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(0);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease2")).isEmpty();

        assertThat(kvSnapshotLeaseManager.dropLease("non-exist")).isFalse();
    }

    @Test
    void testExpireLeases() throws Exception {
        // test lease expire by expire thread.
        Map<Long, List<TableBucketSnapshot>> tableIdToLeaseBucket = initRegisterBuckets();

        // expire after 1000ms.
        kvSnapshotLeaseManager.acquireLease("lease1", 1000L, tableIdToLeaseBucket);

        tableIdToLeaseBucket = new HashMap<>();
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b0, new BucketSnapshot(1L, 10L, "test-path"));
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b1, new BucketSnapshot(1L, 10L, "test-path"));
        tableIdToLeaseBucket.put(
                PARTITION_TABLE_ID,
                Arrays.asList(
                        new TableBucketSnapshot(t0b0, 0L), // same ref.
                        new TableBucketSnapshot(t1p0b0, 1L),
                        new TableBucketSnapshot(t1p0b1, 1L)));
        // expire after 2000ms.
        kvSnapshotLeaseManager.acquireLease("lease2", 2000L, tableIdToLeaseBucket);

        clearLeaseScheduler.triggerPeriodicScheduledTasks();
        // no lease expire.
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(8);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(2);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease1")).isPresent();
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease2")).isPresent();

        manualClock.advanceTime(1005L, TimeUnit.MILLISECONDS);
        clearLeaseScheduler.triggerPeriodicScheduledTasks();
        // lease1 expire.
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(3);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(1);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease1")).isNotPresent();
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease2")).isPresent();

        manualClock.advanceTime(1005L, TimeUnit.MILLISECONDS);
        clearLeaseScheduler.triggerPeriodicScheduledTasks();
        // lease2 expire.
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(0);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(0);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease1")).isNotPresent();
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease2")).isNotPresent();
    }

    @Test
    void registerWithNotExistSnapshotId() throws Exception {
        Map<Long, List<TableBucketSnapshot>> tableIdToRegisterBucket = new HashMap<>();
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Arrays.asList(
                        new TableBucketSnapshot(t0b0, 1000L),
                        new TableBucketSnapshot(t0b1, 1000L)));

        // TODO  this case will return t0b0, t0b1, after we add the checker the check whether
        // snapshot exists.
        assertThat(
                        kvSnapshotLeaseManager
                                .acquireLease("lease1", 1000L, tableIdToRegisterBucket)
                                .keySet())
                .isEmpty();
    }

    private Map<Long, List<TableBucketSnapshot>> initRegisterBuckets() {
        Map<Long, List<TableBucketSnapshot>> tableIdToRegisterBucket = new HashMap<>();
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Arrays.asList(
                        new TableBucketSnapshot(t0b0, 0L), new TableBucketSnapshot(t0b1, 0L)));
        tableIdToRegisterBucket.put(
                PARTITION_TABLE_ID,
                Arrays.asList(
                        new TableBucketSnapshot(t1p0b0, 0L),
                        new TableBucketSnapshot(t1p0b1, 0L),
                        new TableBucketSnapshot(t1p1b0, 0L)));
        return tableIdToRegisterBucket;
    }

    private boolean snapshotLeaseNotExists(List<TableBucketSnapshot> bucketList) {
        return bucketList.stream()
                .noneMatch(bucket -> kvSnapshotLeaseManager.snapshotLeaseExist(bucket));
    }

    private boolean snapshotLeaseExists(List<TableBucketSnapshot> bucketList) {
        return bucketList.stream()
                .allMatch(bucket -> kvSnapshotLeaseManager.snapshotLeaseExist(bucket));
    }

    private void acquire(String leaseId, Map<Long, List<TableBucketSnapshot>> tableIdToLeaseBucket)
            throws Exception {
        kvSnapshotLeaseManager.acquireLease(leaseId, 1000L, tableIdToLeaseBucket);
    }

    private void release(String leaseId, List<TableBucket> tableBucketsToRelease) throws Exception {
        kvSnapshotLeaseManager.release(leaseId, tableBucketsToRelease);
    }

    private long acquire(
            KvSnapshotLeaseHandler kvSnapshotLeasehandle, TableBucketSnapshot leaseForBucket) {
        return kvSnapshotLeasehandle.acquireBucket(
                leaseForBucket.getTableBucket(), leaseForBucket.getSnapshotId());
    }

    // ------------------------------------------------------------------------
    //  Bucket Expansion Tests
    // ------------------------------------------------------------------------

    @Test
    void testAcquireBucketExpansionForNonPartitionedTable() throws Exception {
        // Acquire bucket 0 for non-partitioned table → creates array of size 1
        Map<Long, List<TableBucketSnapshot>> buckets = new HashMap<>();
        buckets.put(
                DATA1_TABLE_ID_PK, Collections.singletonList(new TableBucketSnapshot(t0b0, 0L)));
        acquire("lease1", buckets);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(1);

        // Acquire bucket 1 → should expand array from size 1 to size 2
        buckets = new HashMap<>();
        buckets.put(
                DATA1_TABLE_ID_PK, Collections.singletonList(new TableBucketSnapshot(t0b1, 0L)));
        acquire("lease1", buckets);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(2);

        // Acquire bucket 5 (skipping buckets 2,3,4) → should expand array from size 2 to size 6
        TableBucket t0b5 = new TableBucket(DATA1_TABLE_ID_PK, 5);
        zookeeperClient.registerTableBucketSnapshot(t0b5, new BucketSnapshot(0L, 0L, "test-path"));
        buckets = new HashMap<>();
        buckets.put(
                DATA1_TABLE_ID_PK, Collections.singletonList(new TableBucketSnapshot(t0b5, 0L)));
        acquire("lease1", buckets);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(3);

        // Verify the array content: bucket 0,1 = 0L, bucket 2-4 = -1L, bucket 5 = 0L
        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        tableIdToTableLease.put(
                DATA1_TABLE_ID_PK,
                new KvSnapshotTableLease(
                        DATA1_TABLE_ID_PK, new Long[] {0L, 0L, -1L, -1L, -1L, 0L}));
        KvSnapshotLeaseHandler expectedLease =
                new KvSnapshotLeaseHandler(manualClock.milliseconds() + 1000L, tableIdToTableLease);
        assertThat(kvSnapshotLeaseManager.getKvSnapshotLeaseData("lease1"))
                .isEqualTo(expectedLease);
    }

    @Test
    void testAcquireBucketExpansionForPartitionedTable() throws Exception {
        // Acquire partition bucket 0 → creates array of size 1
        Map<Long, List<TableBucketSnapshot>> buckets = new HashMap<>();
        buckets.put(
                PARTITION_TABLE_ID, Collections.singletonList(new TableBucketSnapshot(t1p0b0, 0L)));
        acquire("lease1", buckets);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(1);

        // Acquire partition bucket 3 (skipping bucket 1,2) → should expand array to size 4
        TableBucket t1p0b3 = new TableBucket(PARTITION_TABLE_ID, PARTITION_ID_1, 3);
        zookeeperClient.registerTableBucketSnapshot(
                t1p0b3, new BucketSnapshot(0L, 0L, "test-path"));
        buckets = new HashMap<>();
        buckets.put(
                PARTITION_TABLE_ID, Collections.singletonList(new TableBucketSnapshot(t1p0b3, 0L)));
        acquire("lease1", buckets);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(2);

        // Verify the array content: bucket 0 = 0L, bucket 1,2 = -1L, bucket 3 = 0L
        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        KvSnapshotTableLease leaseForPartitionTable = new KvSnapshotTableLease(PARTITION_TABLE_ID);
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_1, new Long[] {0L, -1L, -1L, 0L});
        tableIdToTableLease.put(PARTITION_TABLE_ID, leaseForPartitionTable);
        KvSnapshotLeaseHandler expectedLease =
                new KvSnapshotLeaseHandler(manualClock.milliseconds() + 1000L, tableIdToTableLease);
        assertThat(kvSnapshotLeaseManager.getKvSnapshotLeaseData("lease1"))
                .isEqualTo(expectedLease);
    }

    @Test
    void testReleaseBucketExceedingArraySize() throws Exception {
        // Acquire bucket 0 only → array size 1
        Map<Long, List<TableBucketSnapshot>> buckets = new HashMap<>();
        buckets.put(
                DATA1_TABLE_ID_PK, Collections.singletonList(new TableBucketSnapshot(t0b0, 0L)));
        acquire("lease1", buckets);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(1);

        // Release bucket 1 which exceeds array size → should not affect existing leases
        release("lease1", Collections.singletonList(t0b1));
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(1);

        // Release bucket 0 → should remove the lease entry
        release("lease1", Collections.singletonList(t0b0));
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(0);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(0);
    }
}
