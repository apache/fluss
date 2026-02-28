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

package org.apache.fluss.server.zk;

import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.AsyncCallback;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooDefs;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to demonstrate the CuratorCache race condition where NODE_CHANGED events can be lost.
 *
 * <p>Background: CuratorCache internally uses async {@code getData()} calls to read the current ZK
 * state when processing watcher events. It then compares the fetched data's version with the cached
 * version to decide whether to fire NODE_CHANGED. When two ZK mutations happen in quick succession
 * (e.g., node created empty via {@code creatingParentsIfNeeded}, then immediately {@code setData}
 * with real data), the async {@code getData()} for the first event (NodeCreated) may be processed
 * by ZK after the second mutation (setData) has already completed. In this case:
 *
 * <ol>
 *   <li>The getData callback for NodeCreated reads version=1 (final data) and fires NODE_CREATED
 *       with full data
 *   <li>The getData callback for NodeDataChanged also reads version=1, same version as cache, so no
 *       NODE_CHANGED event is fired
 * </ol>
 *
 * <p>To reliably reproduce this race, we use <b>two separate CuratorFramework clients</b> (two ZK
 * sessions) and the <b>raw ZK async API</b> to pipeline create + setData requests. The CuratorCache
 * uses one session for its async {@code getData()} calls, while mutations go through a different
 * session. By pipelining create and setData on the mutation session (sending them back-to-back
 * without waiting for responses), the ZK server processes both before the cache session can even
 * send its async getData. This guarantees that the cache's getData reads the post-setData state.
 */
public class CuratorCacheRaceConditionTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    /**
     * Demonstrates that CuratorCache can fire NODE_CREATED with full data (version > 0) for a node
     * that was initially created as an empty node, and consequently miss the NODE_CHANGED event
     * entirely.
     *
     * <p>This simulates the exact pattern used in {@code MetadataManager.createTable}:
     *
     * <ol>
     *   <li>{@code registerFirstSchema} creates a child node with {@code creatingParentsIfNeeded},
     *       which implicitly creates the parent (table) node as empty (version 0)
     *   <li>{@code registerTable} calls {@code setData} on the parent (table) node with real data
     *       (version becomes 1)
     * </ol>
     *
     * <p>The test uses the raw ZK async API to pipeline create + setData on the mutation session,
     * ensuring they are sent back-to-back on the TCP connection. The ZK server processes them in
     * order: create (version=0) → setData (version=1). By the time the CuratorCache's async getData
     * (from a different session) reaches the server, setData has already been processed, so getData
     * returns version=1. This causes NODE_CREATED to carry version=1, and the subsequent
     * NODE_CHANGED is suppressed because the version hasn't changed from the cache's perspective.
     */
    @Test
    void testNodeChangedEventCanBeLostDueToAsyncGetData() throws Exception {
        String connectString = ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString();
        String basePath = "/test_curator_cache_race";

        // Create two SEPARATE CuratorFramework clients (two ZK sessions, no namespace).
        // - mutationClient: for write operations (create + setData)
        // - cacheClient: for CuratorCache (watches and async getData)
        // Using separate sessions means their requests go through different TCP connections
        // and are NOT serialized at the ZK server.
        CuratorFramework mutationClient =
                CuratorFrameworkFactory.builder()
                        .connectString(connectString)
                        .retryPolicy(new ExponentialBackoffRetry(100, 3))
                        .build();
        mutationClient.start();
        mutationClient.blockUntilConnected();

        CuratorFramework cacheClient =
                CuratorFrameworkFactory.builder()
                        .connectString(connectString)
                        .retryPolicy(new ExponentialBackoffRetry(100, 3))
                        .build();
        cacheClient.start();
        cacheClient.blockUntilConnected();

        try {
            // Clean up from previous runs
            try {
                mutationClient.delete().deletingChildrenIfNeeded().forPath(basePath);
            } catch (Exception ignored) {
            }
            mutationClient.create().forPath(basePath);

            // Track events received by CuratorCache
            List<EventRecord> events = new CopyOnWriteArrayList<>();

            CuratorCache cache = CuratorCache.build(cacheClient, basePath);
            cache.listenable()
                    .addListener(
                            (type, oldData, newData) -> {
                                if (newData != null && !newData.getPath().equals(basePath)) {
                                    int version =
                                            newData.getStat() != null
                                                    ? newData.getStat().getVersion()
                                                    : -1;
                                    int dataLen =
                                            newData.getData() != null
                                                    ? newData.getData().length
                                                    : 0;
                                    events.add(
                                            new EventRecord(
                                                    type, newData.getPath(), version, dataLen));
                                }
                            });
            cache.start();

            // Wait for initial cache build to complete
            retry(Duration.ofMinutes(1), () -> assertThat(cache.get(basePath)).isPresent());

            int totalTables = 500;
            byte[] tableData = "table-registration-data-payload".getBytes();

            // Use the raw ZK async API to pipeline create + setData for each node.
            // Both requests are submitted to the ZK client's send queue immediately,
            // sent back-to-back on the TCP connection. The ZK server processes them in
            // order: create(t_i) → setData(t_i) → create(t_i+1) → setData(t_i+1) → ...
            //
            // When CuratorCache receives the NodeCreated watcher for t_i and sends its
            // async getData, the setData for t_i has ALREADY been processed by ZK
            // (because it was pipelined right after the create on the mutation session).
            // So getData reads version=1, causing NODE_CREATED with version=1 and
            // suppressing the subsequent NODE_CHANGED.
            ZooKeeper zk = mutationClient.getZookeeperClient().getZooKeeper();
            CountDownLatch opsLatch = new CountDownLatch(totalTables * 2);
            AsyncCallback.StringCallback createCb = (rc, path, ctx, name) -> opsLatch.countDown();
            AsyncCallback.StatCallback setDataCb = (rc, path, ctx, stat) -> opsLatch.countDown();

            for (int i = 0; i < totalTables; i++) {
                String tablePath = basePath + "/t_" + i;
                // Pipeline: create (empty, version 0) then setData (version 1) back-to-back
                zk.create(
                        tablePath,
                        new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT,
                        createCb,
                        null);
                zk.setData(tablePath, tableData, -1, setDataCb, null);
            }

            assertThat(opsLatch.await(30, TimeUnit.SECONDS))
                    .as("All ZK operations should complete within 30s")
                    .isTrue();

            // Wait for CuratorCache to process events and verify the race condition.
            // Use retry instead of Thread.sleep to avoid flakiness.
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        int racesDetected = 0;
                        int nodeCreatedWithVersion0 = 0;
                        int nodeCreatedWithVersionGt0 = 0;

                        for (int i = 0; i < totalTables; i++) {
                            String tablePath = basePath + "/t_" + i;

                            boolean hasNodeCreatedWithData = false;
                            boolean hasNodeChanged = false;

                            for (EventRecord e : events) {
                                if (e.path.equals(tablePath)) {
                                    if (e.type == CuratorCacheListener.Type.NODE_CREATED) {
                                        if (e.version > 0) {
                                            hasNodeCreatedWithData = true;
                                            nodeCreatedWithVersionGt0++;
                                        } else {
                                            nodeCreatedWithVersion0++;
                                        }
                                    } else if (e.type == CuratorCacheListener.Type.NODE_CHANGED) {
                                        hasNodeChanged = true;
                                    }
                                }
                            }

                            if (hasNodeCreatedWithData && !hasNodeChanged) {
                                racesDetected++;
                            }
                        }

                        assertThat(racesDetected)
                                .as(
                                        "Expected at least one table to have NODE_CHANGED lost due to "
                                                + "CuratorCache async getData race. "
                                                + "This proves that when a node is created (version=0) and "
                                                + "setData immediately follows (version=1), CuratorCache's "
                                                + "async getData() may read the post-setData state (version>0), "
                                                + "causing the NODE_CREATED event to carry the final data and "
                                                + "the subsequent NODE_CHANGED to be suppressed. "
                                                + "Total tables: "
                                                + totalTables
                                                + ", NODE_CREATED version=0: "
                                                + nodeCreatedWithVersion0
                                                + ", version>0: "
                                                + nodeCreatedWithVersionGt0)
                                .isGreaterThan(0);
                    });

            cache.close();
        } finally {
            // Cleanup
            try {
                mutationClient.delete().deletingChildrenIfNeeded().forPath(basePath);
            } catch (Exception ignored) {
            }
            mutationClient.close();
            cacheClient.close();
        }
    }

    /** Records a CuratorCache event for later analysis. */
    private static class EventRecord {
        final CuratorCacheListener.Type type;
        final String path;
        final int version;
        final int dataLen;

        EventRecord(CuratorCacheListener.Type type, String path, int version, int dataLen) {
            this.type = type;
            this.path = path;
            this.version = version;
            this.dataLen = dataLen;
        }

        @Override
        public String toString() {
            return String.format(
                    "EventRecord{type=%s, path=%s, version=%d, dataLen=%d}",
                    type, path, version, dataLen);
        }
    }
}
