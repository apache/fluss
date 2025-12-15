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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.KvSnapshotLeaseForBucket;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLease;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLeaseMetadataManager;
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
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_ID;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_INFO;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_PATH;
import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.createServers;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvSnapshotLeaseManager}. */
public class KvSnapshotLeaseManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final long PARTITION_ID_1 = 19001L;
    private static final PhysicalTablePath PARTITION_TABLE_PATH_1 =
            PhysicalTablePath.of(PARTITION_TABLE_PATH, "2024");

    private static final long PARTITION_ID_2 = 19002L;
    private static final PhysicalTablePath PARTITION_TABLE_PATH_2 =
            PhysicalTablePath.of(PARTITION_TABLE_PATH, "2025");

    private static final int NUM_BUCKETS = DATA1_TABLE_INFO_PK.getNumBuckets();
    private static final TableBucket t0b0 = new TableBucket(DATA1_TABLE_ID_PK, 0);
    private static final TableBucket t0b1 = new TableBucket(DATA1_TABLE_ID_PK, 1);
    private static final TableBucket t1p0b0 =
            new TableBucket(PARTITION_TABLE_ID, PARTITION_ID_1, 0);
    private static final TableBucket t1p0b1 =
            new TableBucket(PARTITION_TABLE_ID, PARTITION_ID_1, 1);
    private static final TableBucket t1p1b0 =
            new TableBucket(PARTITION_TABLE_ID, PARTITION_ID_2, 0);

    protected static ZooKeeperClient zookeeperClient;

    private CoordinatorContext coordinatorContext;
    private ManualClock manualClock;
    private ManuallyTriggeredScheduledExecutorService clearLeaseScheduler;
    private KvSnapshotLeaseManager kvSnapshotLeaseManager;
    private KvSnapshotLeaseMetadataManager metadataManager;

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
        initCoordinatorContext();
        Configuration conf = new Configuration();
        // set a huge expiration check interval to avoid expiration check.
        conf.set(ConfigOptions.KV_SNAPSHOT_LEASE_EXPIRATION_CHECK_INTERVAL, Duration.ofDays(7));
        manualClock = new ManualClock(System.currentTimeMillis());
        clearLeaseScheduler = new ManuallyTriggeredScheduledExecutorService();
        metadataManager = new KvSnapshotLeaseMetadataManager(zookeeperClient, tempDir.toString());
        kvSnapshotLeaseManager =
                new KvSnapshotLeaseManager(
                        conf,
                        metadataManager,
                        coordinatorContext,
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
                                        new KvSnapshotLeaseForBucket(t0b0, 0L),
                                        new KvSnapshotLeaseForBucket(t0b1, 0L),
                                        new KvSnapshotLeaseForBucket(t1p0b0, 0L),
                                        new KvSnapshotLeaseForBucket(t1p0b1, 0L),
                                        new KvSnapshotLeaseForBucket(t1p1b0, 0L))))
                .isTrue();
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(0);

        // test initialize from zookeeper when coordinator is started.
        KvSnapshotLease kvSnapshotLease = new KvSnapshotLease(1000L);
        acquire(kvSnapshotLease, new KvSnapshotLeaseForBucket(t0b0, 0L));
        acquire(kvSnapshotLease, new KvSnapshotLeaseForBucket(t0b1, 0L));
        acquire(kvSnapshotLease, new KvSnapshotLeaseForBucket(t1p0b0, 0L));
        acquire(kvSnapshotLease, new KvSnapshotLeaseForBucket(t1p0b1, 0L));
        acquire(kvSnapshotLease, new KvSnapshotLeaseForBucket(t1p1b0, 0L));
        metadataManager.registerLease("lease1", kvSnapshotLease);

        kvSnapshotLeaseManager.initialize();

        assertThat(
                        snapshotLeaseExists(
                                Arrays.asList(
                                        new KvSnapshotLeaseForBucket(t0b0, 0L),
                                        new KvSnapshotLeaseForBucket(t0b1, 0L),
                                        new KvSnapshotLeaseForBucket(t1p0b0, 0L),
                                        new KvSnapshotLeaseForBucket(t1p0b1, 0L),
                                        new KvSnapshotLeaseForBucket(t1p1b0, 0L))))
                .isTrue();
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(5);

        // check detail content.
        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        tableIdToTableLease.put(
                DATA1_TABLE_ID_PK,
                new KvSnapshotTableLease(DATA1_TABLE_ID_PK, new Long[] {0L, 0L, -1L}));
        KvSnapshotTableLease leaseForPartitionTable = new KvSnapshotTableLease(PARTITION_TABLE_ID);
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_1, new Long[] {0L, 0L, -1L});
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_2, new Long[] {0L, -1L, -1L});
        tableIdToTableLease.put(PARTITION_TABLE_ID, leaseForPartitionTable);
        KvSnapshotLease expectedLease = new KvSnapshotLease(1000L, tableIdToTableLease);
        assertThat(kvSnapshotLeaseManager.getKvSnapshotLease("lease1")).isEqualTo(expectedLease);
        assertThat(metadataManager.getLease("lease1")).hasValue(expectedLease);
    }

    @Test
    void testAcquireAndRelease() throws Exception {
        Map<Long, List<KvSnapshotLeaseForBucket>> tableIdToRegisterBucket = initRegisterBuckets();
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
                        new KvSnapshotLeaseForBucket(t1p0b0, 1L),
                        new KvSnapshotLeaseForBucket(t1p0b1, 1L)));
        acquire("lease2", tableIdToRegisterBucket);

        assertThat(
                        snapshotLeaseExists(
                                Arrays.asList(
                                        new KvSnapshotLeaseForBucket(t0b0, 0L),
                                        new KvSnapshotLeaseForBucket(t0b1, 0L),
                                        new KvSnapshotLeaseForBucket(t1p0b0, 0),
                                        new KvSnapshotLeaseForBucket(t1p0b1, 0L),
                                        new KvSnapshotLeaseForBucket(t1p1b0, 0L),
                                        new KvSnapshotLeaseForBucket(t1p0b0, 1L),
                                        new KvSnapshotLeaseForBucket(t1p0b1, 1L))))
                .isTrue();
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(7);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(2);

        // update lease register.
        tableIdToRegisterBucket = new HashMap<>();
        zookeeperClient.registerTableBucketSnapshot(t0b0, new BucketSnapshot(1L, 10L, "test-path"));
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Collections.singletonList(new KvSnapshotLeaseForBucket(t0b0, 1L)));
        acquire("lease1", tableIdToRegisterBucket);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(7);

        // new insert.
        tableIdToRegisterBucket = new HashMap<>();
        TableBucket newTableBucket = new TableBucket(DATA1_TABLE_ID_PK, 2);

        zookeeperClient.registerTableBucketSnapshot(
                newTableBucket, new BucketSnapshot(1L, 10L, "test-path"));
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Collections.singletonList(new KvSnapshotLeaseForBucket(newTableBucket, 1L)));
        acquire("lease1", tableIdToRegisterBucket);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(8);

        // release
        Map<Long, List<TableBucket>> tableIdToUnregisterBucket = new HashMap<>();
        tableIdToUnregisterBucket.put(DATA1_TABLE_ID_PK, Collections.singletonList(newTableBucket));
        release("lease1", tableIdToUnregisterBucket);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(7);

        // release a non-exist bucket.
        tableIdToUnregisterBucket = new HashMap<>();
        tableIdToUnregisterBucket.put(
                DATA1_TABLE_ID_PK,
                Collections.singletonList(new TableBucket(DATA1_TABLE_ID_PK, PARTITION_ID_1, 2)));
        release("lease1", tableIdToUnregisterBucket);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(7);

        // check detail content for lease1.
        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        tableIdToTableLease.put(
                DATA1_TABLE_ID_PK,
                new KvSnapshotTableLease(DATA1_TABLE_ID_PK, new Long[] {1L, 0L, -1L}));
        KvSnapshotTableLease leaseForPartitionTable = new KvSnapshotTableLease(PARTITION_TABLE_ID);
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_1, new Long[] {0L, 0L, -1L});
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_2, new Long[] {0L, -1L, -1L});
        tableIdToTableLease.put(PARTITION_TABLE_ID, leaseForPartitionTable);
        KvSnapshotLease expectedLease =
                new KvSnapshotLease(manualClock.milliseconds() + 1000L, tableIdToTableLease);
        assertThat(kvSnapshotLeaseManager.getKvSnapshotLease("lease1")).isEqualTo(expectedLease);
        assertThat(metadataManager.getLease("lease1")).hasValue(expectedLease);

        // check detail content for lease2.
        tableIdToTableLease = new HashMap<>();
        leaseForPartitionTable = new KvSnapshotTableLease(PARTITION_TABLE_ID);
        leaseForPartitionTable.addPartitionSnapshots(PARTITION_ID_1, new Long[] {1L, 1L, -1L});
        tableIdToTableLease.put(PARTITION_TABLE_ID, leaseForPartitionTable);
        KvSnapshotLease expectedLease2 =
                new KvSnapshotLease(manualClock.milliseconds() + 1000L, tableIdToTableLease);
        assertThat(kvSnapshotLeaseManager.getKvSnapshotLease("lease2")).isEqualTo(expectedLease2);
        assertThat(metadataManager.getLease("lease2")).hasValue(expectedLease2);
    }

    @Test
    void testUnregisterAll() throws Exception {
        Map<Long, List<KvSnapshotLeaseForBucket>> tableIdToRegisterBucket = initRegisterBuckets();
        acquire("lease1", tableIdToRegisterBucket);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(5);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(1);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease1")).isPresent();

        Map<Long, List<TableBucket>> tableIdToUnregisterBucket = new HashMap<>();
        tableIdToUnregisterBucket.put(DATA1_TABLE_ID_PK, Arrays.asList(t0b0, t0b1));
        tableIdToUnregisterBucket.put(PARTITION_TABLE_ID, Arrays.asList(t1p0b0, t1p0b1, t1p1b0));

        // unregister all will clear this lease.
        release("lease1", tableIdToUnregisterBucket);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(0);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(0);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease1")).isNotPresent();
    }

    @Test
    void testClear() throws Exception {
        Map<Long, List<KvSnapshotLeaseForBucket>> tableIdToRegisterBucket = initRegisterBuckets();
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
                        new KvSnapshotLeaseForBucket(t0b0, 0L), // same ref.
                        new KvSnapshotLeaseForBucket(t1p0b0, 1L),
                        new KvSnapshotLeaseForBucket(t1p0b1, 1L)));
        acquire("lease2", tableIdToRegisterBucket);

        assertThat(
                        snapshotLeaseExists(
                                Arrays.asList(
                                        new KvSnapshotLeaseForBucket(t0b0, 0L),
                                        new KvSnapshotLeaseForBucket(t0b1, 0L),
                                        new KvSnapshotLeaseForBucket(t1p0b0, 0L),
                                        new KvSnapshotLeaseForBucket(t1p0b1, 0L),
                                        new KvSnapshotLeaseForBucket(t1p1b0, 0L),
                                        new KvSnapshotLeaseForBucket(t1p0b0, 1L),
                                        new KvSnapshotLeaseForBucket(t1p0b1, 1L))))
                .isTrue();
        assertThat(kvSnapshotLeaseManager.getRefCount(new KvSnapshotLeaseForBucket(t0b0, 0L)))
                .isEqualTo(2);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(8);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(2);

        kvSnapshotLeaseManager.releaseAll("lease1");
        assertThat(kvSnapshotLeaseManager.getRefCount(new KvSnapshotLeaseForBucket(t0b0, 0L)))
                .isEqualTo(1);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(3);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(1);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease1")).isEmpty();

        kvSnapshotLeaseManager.releaseAll("lease2");
        assertThat(kvSnapshotLeaseManager.getRefCount(new KvSnapshotLeaseForBucket(t0b0, 0L)))
                .isEqualTo(0);
        assertThat(kvSnapshotLeaseManager.getLeasedBucketCount()).isEqualTo(0);
        assertThat(kvSnapshotLeaseManager.getLeaseCount()).isEqualTo(0);
        assertThat(zookeeperClient.getKvSnapshotLeaseMetadata("lease2")).isEmpty();

        assertThat(kvSnapshotLeaseManager.releaseAll("non-exist")).isFalse();
    }

    @Test
    void testExpireLeases() throws Exception {
        // test lease expire by expire thread.
        Map<Long, List<KvSnapshotLeaseForBucket>> tableIdToLeaseBucket = initRegisterBuckets();

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
                        new KvSnapshotLeaseForBucket(t0b0, 0L), // same ref.
                        new KvSnapshotLeaseForBucket(t1p0b0, 1L),
                        new KvSnapshotLeaseForBucket(t1p0b1, 1L)));
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
        Map<Long, List<KvSnapshotLeaseForBucket>> tableIdToRegisterBucket = new HashMap<>();
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Arrays.asList(
                        new KvSnapshotLeaseForBucket(t0b0, 1000L),
                        new KvSnapshotLeaseForBucket(t0b1, 1000L)));
        assertThat(
                        kvSnapshotLeaseManager
                                .acquireLease("lease1", 1000L, tableIdToRegisterBucket)
                                .keySet())
                .contains(t0b0);
    }

    private void initCoordinatorContext() {
        coordinatorContext = new CoordinatorContext();
        coordinatorContext.setLiveTabletServers(createServers(Arrays.asList(0, 1, 2)));

        // register an non-partitioned table.
        coordinatorContext.putTableInfo(DATA1_TABLE_INFO_PK);
        coordinatorContext.putTablePath(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK);

        // register a partitioned table.
        coordinatorContext.putTableInfo(PARTITION_TABLE_INFO);
        coordinatorContext.putTablePath(
                PARTITION_TABLE_INFO.getTableId(), PARTITION_TABLE_INFO.getTablePath());
        coordinatorContext.putPartition(PARTITION_ID_1, PARTITION_TABLE_PATH_1);
        coordinatorContext.putPartition(PARTITION_ID_2, PARTITION_TABLE_PATH_2);
    }

    private Map<Long, List<KvSnapshotLeaseForBucket>> initRegisterBuckets() {
        Map<Long, List<KvSnapshotLeaseForBucket>> tableIdToRegisterBucket = new HashMap<>();
        tableIdToRegisterBucket.put(
                DATA1_TABLE_ID_PK,
                Arrays.asList(
                        new KvSnapshotLeaseForBucket(t0b0, 0L),
                        new KvSnapshotLeaseForBucket(t0b1, 0L)));
        tableIdToRegisterBucket.put(
                PARTITION_TABLE_ID,
                Arrays.asList(
                        new KvSnapshotLeaseForBucket(t1p0b0, 0L),
                        new KvSnapshotLeaseForBucket(t1p0b1, 0L),
                        new KvSnapshotLeaseForBucket(t1p1b0, 0L)));
        return tableIdToRegisterBucket;
    }

    private boolean snapshotLeaseNotExists(List<KvSnapshotLeaseForBucket> bucketList) {
        return bucketList.stream()
                .allMatch(bucket -> kvSnapshotLeaseManager.snapshotLeaseNotExist(bucket));
    }

    private boolean snapshotLeaseExists(List<KvSnapshotLeaseForBucket> bucketList) {
        return bucketList.stream()
                .noneMatch(bucket -> kvSnapshotLeaseManager.snapshotLeaseNotExist(bucket));
    }

    private void acquire(
            String leaseId, Map<Long, List<KvSnapshotLeaseForBucket>> tableIdToLeaseBucket)
            throws Exception {
        kvSnapshotLeaseManager.acquireLease(leaseId, 1000L, tableIdToLeaseBucket);
    }

    private void release(String leaseId, Map<Long, List<TableBucket>> tableIdToReleaseBucket)
            throws Exception {
        kvSnapshotLeaseManager.release(leaseId, tableIdToReleaseBucket);
    }

    private long acquire(KvSnapshotLease kvSnapshotLease, KvSnapshotLeaseForBucket leaseForBucket) {
        return kvSnapshotLease.acquireBucket(
                leaseForBucket.getTableBucket(), leaseForBucket.getKvSnapshotId(), NUM_BUCKETS);
    }
}
