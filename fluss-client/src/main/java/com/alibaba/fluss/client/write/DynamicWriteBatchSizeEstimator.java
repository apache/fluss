/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.utils.concurrent.FlussScheduler;
import com.alibaba.fluss.utils.concurrent.Scheduler;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/** An estimator to estimate the buffer usage of a writeBatch. */
@Internal
@ThreadSafe
public class DynamicWriteBatchSizeEstimator {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicWriteBatchSizeEstimator.class);

    private static final int MAX_QUEUE_SIZE = 100;
    private static final double DIFF_RATIO_THRESHOLD = 0.2;
    static final double INCREASE_RATIO = 0.2;
    private final Lock lock = new ReentrantLock();
    private final int initialBatchSize;
    private final int bufferTotalSize;
    private final int pageSize;

    @GuardedBy("lock")
    private final Map<PhysicalTablePath, WriteBatchSizeStatus> writeBatchSizeStatusMap;

    public DynamicWriteBatchSizeEstimator(
            int initialBatchSize, int bufferTotalSize, int pageSize, long estimateInterval) {
        this.writeBatchSizeStatusMap = new HashMap<>();
        this.initialBatchSize = initialBatchSize;
        this.bufferTotalSize = bufferTotalSize;
        this.pageSize = pageSize;

        Scheduler scheduler = new FlussScheduler(1);
        if (estimateInterval > 0) {
            scheduler.startup();
            scheduler.schedule(
                    "updateBatchSize",
                    this::dynamicEstimateBatchSize,
                    estimateInterval,
                    estimateInterval);
        }
    }

    @VisibleForTesting
    void dynamicEstimateBatchSize() {
        Map<PhysicalTablePath, Tuple2<Integer, List<Integer>>> writeBatchSizeStatusSnapshot =
                new HashMap<>();

        inLock(
                lock,
                () -> {
                    for (Map.Entry<PhysicalTablePath, WriteBatchSizeStatus> entry :
                            writeBatchSizeStatusMap.entrySet()) {
                        WriteBatchSizeStatus writeBatchSizeStatus = entry.getValue();
                        int currentBatchSize = writeBatchSizeStatus.currentBatchSize;
                        List<Integer> recentBatchSize =
                                new ArrayList<>(writeBatchSizeStatus.recentBatchSizeQueue);
                        writeBatchSizeStatusSnapshot.put(
                                entry.getKey(), Tuple2.of(currentBatchSize, recentBatchSize));
                    }
                });

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<PhysicalTablePath, Tuple2<Integer, List<Integer>>> entry :
                writeBatchSizeStatusSnapshot.entrySet()) {
            Tuple2<Integer, List<Integer>> tuple2 = entry.getValue();
            int currentBatchSize = tuple2.f0;
            List<Integer> recentBatchSize = tuple2.f1;

            int sum = 0;
            for (int batchSize : recentBatchSize) {
                sum += batchSize;
            }
            double averageBatchSize = (double) sum / recentBatchSize.size();

            double diffRatio = Math.abs(currentBatchSize - averageBatchSize) / currentBatchSize;

            int newBatchSize;
            if (diffRatio <= DIFF_RATIO_THRESHOLD) {
                newBatchSize =
                        Math.min(
                                (int) Math.round(currentBatchSize * (1 + INCREASE_RATIO)),
                                bufferTotalSize);
            } else {
                newBatchSize = Math.max(currentBatchSize / 2, pageSize);
            }
            setCurrentBatchSize(entry.getKey(), newBatchSize);

            if (LOG.isDebugEnabled()) {
                sb.append(entry.getKey())
                        .append(":")
                        .append("batchSize")
                        .append("->")
                        .append(newBatchSize)
                        .append("\n");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("dynamic update batch size to: {}", sb);
        }
    }

    public void recordNewBatchSize(PhysicalTablePath physicalTablePath, int batchSize) {
        inLock(
                lock,
                () -> {
                    WriteBatchSizeStatus writeBatchSizeStatus =
                            writeBatchSizeStatusMap.computeIfAbsent(
                                    physicalTablePath,
                                    k -> new WriteBatchSizeStatus(initialBatchSize));
                    writeBatchSizeStatus.recordNewBatchSize(batchSize);
                });
    }

    public int batchSize(PhysicalTablePath physicalTablePath) {
        return inLock(
                lock,
                () -> {
                    WriteBatchSizeStatus writeBatchSizeStatus =
                            writeBatchSizeStatusMap.computeIfAbsent(
                                    physicalTablePath,
                                    k -> new WriteBatchSizeStatus(initialBatchSize));
                    return writeBatchSizeStatus.currentBatchSize;
                });
    }

    private void setCurrentBatchSize(PhysicalTablePath physicalTablePath, int batchSize) {
        inLock(
                lock,
                () -> {
                    WriteBatchSizeStatus writeBatchSizeStatus =
                            writeBatchSizeStatusMap.get(physicalTablePath);
                    writeBatchSizeStatus.setCurrentBatchSize(batchSize);
                });
    }

    private static final class WriteBatchSizeStatus {

        final Deque<Integer> recentBatchSizeQueue;
        int currentBatchSize;

        WriteBatchSizeStatus(int initialBatchSize) {
            this.currentBatchSize = initialBatchSize;
            this.recentBatchSizeQueue = new ArrayDeque<>(MAX_QUEUE_SIZE);
        }

        void recordNewBatchSize(int newBatchSize) {
            if (recentBatchSizeQueue.size() >= MAX_QUEUE_SIZE) {
                recentBatchSizeQueue.removeFirst();
            }
            recentBatchSizeQueue.addLast(newBatchSize);
        }

        void setCurrentBatchSize(int batchSize) {
            currentBatchSize = batchSize;
        }
    }
}
