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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.ConsumeKvSnapshotForBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.KvSnapshotConsumer;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/** A manager to manage kv snapshot consumer register/unregister/clear. */
@ThreadSafe
public class KvSnapshotConsumerManager {
    private static final Logger LOG = LoggerFactory.getLogger(KvSnapshotConsumerManager.class);

    private final ZooKeeperClient zkClient;
    private final CoordinatorContext coordinatorContext;
    private final Clock clock;
    private final ScheduledExecutorService scheduledExecutor;
    private final Configuration conf;

    private final Map<String, ReadWriteLock> consumerLocks = MapUtils.newConcurrentHashMap();
    /** Consumer id to Consumer. */
    @GuardedBy("consumerLocks")
    private final Map<String, KvSnapshotConsumer> consumers;

    private final ReadWriteLock refCountLock = new ReentrantReadWriteLock();

    /**
     * ConsumeKvSnapshotForBucket to the ref count, which means this table bucket + snapshotId has
     * been consumed by how many consumers.
     */
    @GuardedBy("refCountLock")
    private final Map<ConsumeKvSnapshotForBucket, AtomicInteger> refCount =
            MapUtils.newConcurrentHashMap();

    /** For metrics. */
    private final AtomicInteger consumedBucketCount = new AtomicInteger(0);

    public KvSnapshotConsumerManager(
            Configuration conf,
            ZooKeeperClient zkClient,
            CoordinatorContext coordinatorContext,
            Clock clock,
            CoordinatorMetricGroup coordinatorMetricGroup) {
        this(
                conf,
                zkClient,
                coordinatorContext,
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("kv-snapshot-consumer-cleaner")),
                clock,
                coordinatorMetricGroup);
    }

    @VisibleForTesting
    public KvSnapshotConsumerManager(
            Configuration conf,
            ZooKeeperClient zkClient,
            CoordinatorContext coordinatorContext,
            ScheduledExecutorService scheduledExecutor,
            Clock clock,
            CoordinatorMetricGroup coordinatorMetricGroup) {
        this.zkClient = zkClient;
        this.conf = conf;
        this.scheduledExecutor = scheduledExecutor;
        this.coordinatorContext = coordinatorContext;
        this.clock = clock;
        this.consumers = MapUtils.newConcurrentHashMap();

        registerMetrics(coordinatorMetricGroup);
    }

    public void start() {
        scheduledExecutor.scheduleWithFixedDelay(
                this::expireConsumers,
                0L,
                conf.get(ConfigOptions.KV_SNAPSHOT_CONSUMER_EXPIRATION_CHECK_INTERVAL).toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void initialize() throws Exception {
        List<String> consumers = zkClient.getKvSnapshotConsumerList();
        for (String consumer : consumers) {
            Optional<KvSnapshotConsumer> kvSnapshotConsumerOpt =
                    zkClient.getKvSnapshotConsumer(consumer);
            if (kvSnapshotConsumerOpt.isPresent()) {
                KvSnapshotConsumer kvSnapshotConsumer = kvSnapshotConsumerOpt.get();
                this.consumerLocks.put(consumer, new ReentrantReadWriteLock());
                this.consumers.put(consumer, kvSnapshotConsumer);

                initializeRefCount(kvSnapshotConsumer);

                consumedBucketCount.getAndAdd(kvSnapshotConsumer.getConsumedSnapshotCount());
            }
        }
    }

    public boolean snapshotConsumerNotExist(ConsumeKvSnapshotForBucket consumeKvSnapshotForBucket) {
        return inReadLock(
                refCountLock,
                () ->
                        !refCount.containsKey(consumeKvSnapshotForBucket)
                                || refCount.get(consumeKvSnapshotForBucket).get() <= 0);
    }

    public void register(
            String consumerId,
            long expirationTime,
            Map<Long, List<ConsumeKvSnapshotForBucket>> tableIdToRegisterBucket)
            throws Exception {
        ReadWriteLock lock =
                consumerLocks.computeIfAbsent(consumerId, k -> new ReentrantReadWriteLock());
        inWriteLock(
                lock,
                () -> {
                    boolean update = consumers.containsKey(consumerId);
                    KvSnapshotConsumer consumer;
                    if (!update) {
                        // set the expiration time as: current time + expirationTime
                        consumer = new KvSnapshotConsumer(clock.milliseconds() + expirationTime);
                        consumers.put(consumerId, consumer);
                    } else {
                        consumer = consumers.get(consumerId);
                    }

                    for (Map.Entry<Long, List<ConsumeKvSnapshotForBucket>> entry :
                            tableIdToRegisterBucket.entrySet()) {
                        Long tableId = entry.getKey();
                        TableInfo tableInfo = coordinatorContext.getTableInfoById(tableId);
                        int numBuckets = tableInfo.getNumBuckets();
                        List<ConsumeKvSnapshotForBucket> buckets = entry.getValue();
                        for (ConsumeKvSnapshotForBucket bucket : buckets) {
                            boolean isUpdate =
                                    consumer.registerBucket(
                                            bucket.getTableBucket(),
                                            bucket.getKvSnapshotId(),
                                            numBuckets);
                            if (!isUpdate) {
                                consumedBucketCount.getAndIncrement();
                                inWriteLock(
                                        refCountLock,
                                        () -> {
                                            refCount.computeIfAbsent(
                                                            bucket, k -> new AtomicInteger(0))
                                                    .getAndIncrement();
                                        });
                            }
                        }
                    }

                    if (update) {
                        zkClient.updateKvSnapshotConsumer(consumerId, consumer);
                    } else {
                        zkClient.registerKvSnapshotConsumer(consumerId, consumer);
                    }
                });
    }

    public void unregister(
            String consumerId, Map<Long, List<TableBucket>> tableIdToUnregisterBucket)
            throws Exception {
        ReadWriteLock lock = consumerLocks.get(consumerId);
        if (lock == null) {
            return;
        }

        inWriteLock(
                lock,
                () -> {
                    KvSnapshotConsumer consumer = consumers.get(consumerId);

                    if (consumer == null) {
                        return;
                    }

                    for (Map.Entry<Long, List<TableBucket>> entry :
                            tableIdToUnregisterBucket.entrySet()) {
                        List<TableBucket> buckets = entry.getValue();
                        for (TableBucket bucket : buckets) {
                            long snapshotId = consumer.unregisterBucket(bucket);
                            if (snapshotId != -1L) {
                                consumedBucketCount.getAndDecrement();
                                inWriteLock(
                                        refCountLock,
                                        () -> {
                                            refCount.get(
                                                            new ConsumeKvSnapshotForBucket(
                                                                    bucket, snapshotId))
                                                    .getAndDecrement();
                                        });
                            }
                        }
                    }

                    if (consumer.isEmpty()) {
                        clear(consumerId);
                    } else {
                        zkClient.updateKvSnapshotConsumer(consumerId, consumer);
                    }
                });
    }

    public void clear(String consumerId) throws Exception {
        ReadWriteLock lock = consumerLocks.get(consumerId);
        if (lock == null) {
            return;
        }

        inWriteLock(
                lock,
                () -> {
                    KvSnapshotConsumer kvSnapshotConsumer = consumers.remove(consumerId);
                    clearRefCount(kvSnapshotConsumer);

                    zkClient.deleteKvSnapshotConsumer(consumerId);
                });

        consumerLocks.remove(consumerId);
    }

    private void initializeRefCount(KvSnapshotConsumer consumer) {
        for (Map.Entry<Long, Long[]> entry : consumer.getTableIdToSnapshots().entrySet()) {
            Long[] snapshots = entry.getValue();
            for (int i = 0; i < snapshots.length; i++) {
                if (snapshots[i] == -1L) {
                    continue;
                }

                ConsumeKvSnapshotForBucket bucket =
                        new ConsumeKvSnapshotForBucket(
                                new TableBucket(entry.getKey(), i), snapshots[i]);
                inWriteLock(
                        refCountLock,
                        () ->
                                refCount.computeIfAbsent(bucket, k -> new AtomicInteger(0))
                                        .getAndIncrement());
            }
        }

        Map<Long, Long[]> partitionIdToSnapshots = consumer.getPartitionIdToSnapshots();
        for (Map.Entry<Long, Set<Long>> entry : consumer.getTableIdToPartitions().entrySet()) {
            Long tableId = entry.getKey();
            Set<Long> partitions = entry.getValue();
            for (Long partition : partitions) {
                Long[] snapshots = partitionIdToSnapshots.get(partition);
                for (int i = 0; i < snapshots.length; i++) {
                    if (snapshots[i] == -1L) {
                        continue;
                    }

                    ConsumeKvSnapshotForBucket bucket =
                            new ConsumeKvSnapshotForBucket(
                                    new TableBucket(tableId, partition, i), snapshots[i]);
                    inWriteLock(
                            refCountLock,
                            () ->
                                    refCount.computeIfAbsent(bucket, k -> new AtomicInteger(0))
                                            .getAndIncrement());
                }
            }
        }
    }

    private void clearRefCount(KvSnapshotConsumer consumer) {
        for (Map.Entry<Long, Long[]> entry : consumer.getTableIdToSnapshots().entrySet()) {
            Long[] snapshots = entry.getValue();
            for (int i = 0; i < snapshots.length; i++) {
                if (snapshots[i] == -1L) {
                    continue;
                }

                ConsumeKvSnapshotForBucket bucket =
                        new ConsumeKvSnapshotForBucket(
                                new TableBucket(entry.getKey(), i), snapshots[i]);
                inWriteLock(
                        refCountLock,
                        () -> {
                            int decrementAndGet = refCount.get(bucket).decrementAndGet();
                            if (decrementAndGet <= 0) {
                                refCount.remove(bucket);
                            }
                        });
                consumedBucketCount.getAndDecrement();
            }
        }

        Map<Long, Long[]> partitionIdToSnapshots = consumer.getPartitionIdToSnapshots();
        for (Map.Entry<Long, Set<Long>> entry : consumer.getTableIdToPartitions().entrySet()) {
            Long tableId = entry.getKey();
            Set<Long> partitions = entry.getValue();
            for (Long partition : partitions) {
                Long[] snapshots = partitionIdToSnapshots.get(partition);
                for (int i = 0; i < snapshots.length; i++) {
                    if (snapshots[i] == -1L) {
                        continue;
                    }
                    ConsumeKvSnapshotForBucket bucket =
                            new ConsumeKvSnapshotForBucket(
                                    new TableBucket(tableId, partition, i), snapshots[i]);
                    inWriteLock(
                            refCountLock,
                            () -> {
                                int decrementAndGet = refCount.get(bucket).decrementAndGet();
                                if (decrementAndGet <= 0) {
                                    refCount.remove(bucket);
                                }
                            });
                    consumedBucketCount.getAndDecrement();
                }
            }
        }
    }

    private void expireConsumers() {
        long currentTime = clock.milliseconds();
        List<String> expiredConsumers =
                consumers.entrySet().stream()
                        .filter(entry -> entry.getValue().getExpirationTime() < currentTime)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        for (String consumer : expiredConsumers) {
            try {
                clear(consumer);
            } catch (Exception e) {
                LOG.error("Failed to clear kv snapshot consumer {}", consumer, e);
            }
        }
    }

    private void registerMetrics(CoordinatorMetricGroup coordinatorMetricGroup) {
        coordinatorMetricGroup.gauge(
                MetricNames.KV_SNAPSHOT_CONSUMER_COUNT, this::getConsumerCount);
        // TODO register as table or bucket level.
        coordinatorMetricGroup.gauge(
                MetricNames.CONSUMED_KV_SNAPSHOT_COUNT, this::getConsumedBucketCount);
    }

    @VisibleForTesting
    int getConsumerCount() {
        return consumers.size();
    }

    @VisibleForTesting
    int getConsumedBucketCount() {
        return consumedBucketCount.get();
    }

    @VisibleForTesting
    int getRefCount(ConsumeKvSnapshotForBucket consumeKvSnapshotForBucket) {
        return inReadLock(
                refCountLock,
                () -> {
                    if (!refCount.containsKey(consumeKvSnapshotForBucket)) {
                        return 0;
                    } else {
                        return refCount.get(consumeKvSnapshotForBucket).get();
                    }
                });
    }

    @VisibleForTesting
    KvSnapshotConsumer getKvSnapshotConsumer(String consumerId) {
        return consumers.get(consumerId);
    }
}
