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

package org.apache.fluss.trino;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.scan.ScannerContext;
import org.apache.fluss.server.kv.scan.ScannerManager;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.clock.Clock;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;

/** Observes real bucket sessions and triggers real TTL eviction without production test hooks. */
final class TestingKvScanControl {
    private final ScannerManager manager;
    private final TableBucket bucket;

    private TestingKvScanControl(ScannerManager manager, TableBucket bucket) {
        this.manager = manager;
        this.bucket = bucket;
    }

    static TestingKvScanControl forBucket(FlussClusterExtension cluster, TableBucket bucket)
            throws Exception {
        Replica leader = cluster.waitAndGetLeaderReplica(bucket);
        for (TabletServer server : cluster.getTabletServers()) {
            ReplicaManager replicas = server.getReplicaManager();
            ReplicaManager.HostedReplica hosted = replicas.getReplica(bucket);
            if (hosted instanceof ReplicaManager.OnlineReplica
                    && ((ReplicaManager.OnlineReplica) hosted).getReplica() == leader) {
                return new TestingKvScanControl(
                        (ScannerManager) field(replicas, ReplicaManager.class, "scannerManager"),
                        bucket);
            }
        }
        throw new IllegalStateException("No leader scanner manager for " + bucket);
    }

    int activeScannerCount() {
        return manager.activeScannerCountForBucket(bucket);
    }

    List<byte[]> scannerIds() throws Exception {
        List<byte[]> result = new ArrayList<>();
        for (ScannerContext context : contexts()) {
            result.add(context.getScannerId().clone());
        }
        return result;
    }

    void awaitCallSequence(int sequence) throws Exception {
        waitUntil(
                () -> {
                    for (ScannerContext context : contexts()) {
                        if (context.tryAcquireForUse()) {
                            try {
                                if (context.getCallSeqId() >= sequence) {
                                    return true;
                                }
                            } finally {
                                context.releaseAfterUse();
                            }
                        }
                    }
                    return false;
                },
                Duration.ofSeconds(30),
                "Scanner did not finish expected request for " + bucket);
    }

    void expireAll() throws Exception {
        Clock clock = (Clock) field(manager, ScannerManager.class, "clock");
        long ttl = (Long) field(manager, ScannerManager.class, "scannerTtlMs");
        for (ScannerContext context : contexts()) {
            waitUntil(context::tryAcquireForUse, Duration.ofSeconds(30), "Scanner remains in use");
            try {
                context.updateLastAccessTime(clock.milliseconds() - ttl - 1);
            } finally {
                // Eviction closes the context and must not hold its cursor-use fence.
                context.releaseAfterUse();
            }
        }
        Method evict = ScannerManager.class.getDeclaredMethod("evictExpiredScanners");
        evict.setAccessible(true);
        evict.invoke(manager);
    }

    void removeAll() throws Exception {
        for (byte[] id : scannerIds()) {
            manager.removeScanner(id);
        }
    }

    private List<ScannerContext> contexts() throws Exception {
        Map<?, ?> sessions = (Map<?, ?>) field(manager, ScannerManager.class, "scanners");
        List<ScannerContext> result = new ArrayList<>();
        for (Object value : sessions.values()) {
            ScannerContext context = (ScannerContext) value;
            if (bucket.equals(context.getTableBucket())) {
                result.add(context);
            }
        }
        return result;
    }

    private static Object field(Object target, Class<?> owner, String name) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
