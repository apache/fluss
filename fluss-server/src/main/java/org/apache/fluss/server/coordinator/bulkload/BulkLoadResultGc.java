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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.DataWithStat;
import org.apache.fluss.server.zk.ZooKeeperOp;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/** Collects expired BulkLoad results without blocking the Coordinator event thread. */
@Internal
public final class BulkLoadResultGc {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadResultGc.class);
    private final ZooKeeperClient zkClient;
    private final ZooKeeperOp zkOp;
    private final Executor ioExecutor;
    private final long refreshIntervalMs;
    private final AtomicBoolean running = new AtomicBoolean();

    // Accessed only by the single in-flight I/O task.
    private Iterator<Candidate> candidates = Collections.emptyIterator();
    private long refreshNotBeforeMs;

    /** Creates a collector using the coordinator I/O executor. */
    public BulkLoadResultGc(ZooKeeperClient zkClient, Executor ioExecutor) {
        this(zkClient, ioExecutor, 1_000L);
    }

    @VisibleForTesting
    BulkLoadResultGc(ZooKeeperClient zkClient, Executor ioExecutor, long refreshIntervalMs) {
        this.zkClient = zkClient;
        this.zkOp = new ZooKeeperOp(zkClient.getCuratorClient());
        this.ioExecutor = ioExecutor;
        this.refreshIntervalMs = refreshIntervalMs;
    }

    /** Schedules at most one candidate; epoch-checked writes reject a stale Coordinator. */
    public void runMaintenance(long nowMs, int coordinatorEpochVersion) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            ioExecutor.execute(
                    () -> {
                        try {
                            if (!candidates.hasNext() && nowMs >= refreshNotBeforeMs) {
                                refreshNotBeforeMs = nowMs + refreshIntervalMs;
                                candidates = enumerateCandidates().iterator();
                            }
                            if (candidates.hasNext()) {
                                Candidate candidate = candidates.next();
                                if (candidate.emptyParent) {
                                    // ZooKeeper rejects deletion if a concurrent Begin added a
                                    // child.
                                    zkClient.submitCheckedMulti(
                                            Arrays.asList(
                                                    zkOp.checkOp(
                                                            ZkData.CoordinatorEpochZNode.path(),
                                                            coordinatorEpochVersion),
                                                    zkOp.deleteOp(candidate.path, -1)));
                                } else {
                                    collect(candidate.path, nowMs, coordinatorEpochVersion);
                                }
                            }
                        } catch (Exception failure) {
                            LOG.debug(
                                    "BulkLoad result GC candidate is not currently deletable.",
                                    failure);
                        } finally {
                            running.set(false);
                        }
                    });
        } catch (RuntimeException rejected) {
            running.set(false);
            LOG.warn("Failed to schedule BulkLoad result GC.", rejected);
        }
    }

    private List<Candidate> enumerateCandidates() throws Exception {
        List<Candidate> result = new ArrayList<>();
        for (String scope :
                Arrays.asList(
                        ZkData.BulkLoadTableTransactionsZNode.path(),
                        ZkData.BulkLoadPartitionTransactionsZNode.path())) {
            for (String physicalId : zkClient.getChildren(scope)) {
                String parent = scope + "/" + physicalId;
                List<String> transactions = zkClient.getChildren(parent);
                if (transactions.isEmpty()) {
                    result.add(new Candidate(parent, true));
                } else {
                    for (String id : transactions) {
                        result.add(new Candidate(parent + "/" + id, false));
                    }
                }
            }
        }
        return result;
    }

    private void collect(String path, long nowMs, int coordinatorEpochVersion) throws Exception {
        Optional<DataWithStat> data = zkClient.getDataWithStatIfExists(path);
        if (!data.isPresent()) {
            return;
        }
        BulkLoadTransaction transaction =
                ZkData.BulkLoadTransactionsZNode.decode(data.get().getData());
        if (!transaction.getState().isTerminal() || nowMs < transaction.getDeadlineMs()) {
            return;
        }
        List<CuratorOp> operations = new ArrayList<>();
        operations.add(zkOp.checkOp(ZkData.CoordinatorEpochZNode.path(), coordinatorEpochVersion));
        operations.add(zkOp.deleteOp(path, data.get().getStat().getVersion()));
        zkClient.submitCheckedMulti(operations);
    }

    private static final class Candidate {
        private final String path;
        private final boolean emptyParent;

        private Candidate(String path, boolean emptyParent) {
            this.path = path;
            this.emptyParent = emptyParent;
        }
    }
}
