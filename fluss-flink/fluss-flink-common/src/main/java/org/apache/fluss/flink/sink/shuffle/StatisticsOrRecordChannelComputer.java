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

package org.apache.fluss.flink.sink.shuffle;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.ChannelComputer;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * {@link ChannelComputer} for {@link StatisticsOrRecord} which will change shuffle based on
 * partition statistic.
 */
@Internal
public class StatisticsOrRecordChannelComputer<InputT>
        implements ChannelComputer<StatisticsOrRecord<InputT>> {
    private static final Logger LOG =
            LoggerFactory.getLogger(StatisticsOrRecordChannelComputer.class);

    private final @Nullable DataLakeFormat lakeFormat;
    private final RowType flussRowType;
    private final List<String> bucketKeys;
    private final List<String> partitionKeys;
    private final FlussSerializationSchema<InputT> serializationSchema;
    private final int bucketNum;

    private transient int downstreamNumChannels;
    private transient KeyEncoder bucketKeyEncoder;
    private transient PartitionGetter partitionGetter;
    private transient MapPartitioner delegatePartitioner;
    private transient AtomicLong roundRobinCounter;
    private transient BucketingFunction bucketingFunction;
    private transient Random random;

    public StatisticsOrRecordChannelComputer(
            RowType flussRowType,
            List<String> bucketKeys,
            List<String> partitionKeys,
            int bucketNum,
            @Nullable DataLakeFormat lakeFormat,
            FlussSerializationSchema<InputT> serializationSchema) {
        checkArgument(
                partitionKeys != null && !partitionKeys.isEmpty(),
                "Partition keys cannot be empty.");
        this.flussRowType = flussRowType;
        this.bucketKeys = bucketKeys;
        this.partitionKeys = partitionKeys;
        this.bucketNum = bucketNum;
        this.lakeFormat = lakeFormat;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void setup(int numChannels) {
        LOG.info("Setting up with {} downstream channels", numChannels);
        this.downstreamNumChannels = numChannels;
        this.bucketingFunction = BucketingFunction.of(lakeFormat);
        this.bucketKeyEncoder = KeyEncoder.of(flussRowType, bucketKeys, lakeFormat);
        this.partitionGetter = new PartitionGetter(flussRowType, partitionKeys);
        try {
            this.serializationSchema.open(new SerializerInitContextImpl(flussRowType));
        } catch (Exception e) {
            throw new FlussRuntimeException(e);
        }
        this.random = ThreadLocalRandom.current();
    }

    @Override
    public int channel(StatisticsOrRecord<InputT> wrapper) {
        if (wrapper == null) {
            throw new FlussRuntimeException("StatisticsOrRecord wrapper must not be null");
        }

        try {
            if (wrapper.isStatistics()) {
                this.delegatePartitioner = delegatePartitioner(wrapper.statistics());
                return (int)
                        (roundRobinCounter(downstreamNumChannels).getAndIncrement()
                                % downstreamNumChannels);
            } else {
                if (delegatePartitioner == null) {
                    delegatePartitioner = delegatePartitioner(null);
                }

                RowWithOp rowWithOp = serializationSchema.serialize(wrapper.record());
                InternalRow row = rowWithOp.getRow();
                String partitionName = partitionGetter.getPartition(row);
                return delegatePartitioner.select(partitionName, row, downstreamNumChannels);
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to serialize static or record of type '%s' in FlinkRowDataChannelComputer: %s",
                            wrapper.getClass().getName(), e.getMessage()),
                    e);
        }
    }

    private boolean hasBucketKey() {
        return bucketKeys != null && !bucketKeys.isEmpty();
    }

    private AtomicLong roundRobinCounter(int numPartitions) {
        if (roundRobinCounter == null) {
            // randomize the starting point to avoid synchronization across subtasks
            this.roundRobinCounter = new AtomicLong(new Random().nextInt(numPartitions));
        }

        return roundRobinCounter;
    }

    private MapPartitioner delegatePartitioner(@Nullable DataStatistics statistics) {
        return statistics == null
                ? new MapPartitioner()
                : new MapPartitioner(assignment(downstreamNumChannels, statistics.result()));
    }

    Map<String, PartitionAssignment> assignment(
            int downstreamParallelism, Map<String, Long> statistics) {
        statistics.forEach(
                (key, value) ->
                        checkArgument(
                                value > 0, "Invalid statistics: weight is 0 for key %s", key));

        long totalWeight = statistics.values().stream().mapToLong(l -> l).sum();
        long targetWeightPerSubtask =
                (long) Math.ceil(((double) totalWeight) / downstreamParallelism);

        return buildAssignment(
                downstreamParallelism,
                statistics,
                targetWeightPerSubtask,
                hasBucketKey(),
                bucketNum);
    }

    Map<String, PartitionAssignment> buildAssignment(
            int downstreamParallelism,
            Map<String, Long> dataStatistics,
            long targetWeightPerSubtask,
            boolean hasBucketKey,
            int bucketNum) {
        Map<String, PartitionAssignment> assignmentMap = new HashMap<>(dataStatistics.size());
        Iterator<String> mapKeyIterator = dataStatistics.keySet().iterator();
        int subtaskId = 0;
        String currentKey = null;
        long keyRemainingWeight = 0L;
        long subtaskRemainingWeight = targetWeightPerSubtask;
        // todo: 计算assigned 的 subtasks 列表，并计算每个subtask的 weight
        List<Integer> assignedSubtasks = new ArrayList<>();
        List<Long> subtaskWeights = new ArrayList<>();
        while (mapKeyIterator.hasNext() || currentKey != null) {
            // This should never happen because target weight is calculated using ceil function.
            // todo: numPartitions是下游的所有id
            if (subtaskId >= downstreamParallelism) {
                LOG.error(
                        "Internal algorithm error: exhausted subtasks with unassigned keys left. number of partitions: {}, "
                                + "target weight per subtask: {}, data statistics: {}",
                        downstreamParallelism,
                        targetWeightPerSubtask,
                        dataStatistics);
                throw new IllegalStateException(
                        "Internal algorithm error: exhausted subtasks with unassigned keys left");
            }

            if (currentKey == null) {
                currentKey = mapKeyIterator.next();
                keyRemainingWeight = dataStatistics.get(currentKey);
            }

            assignedSubtasks.add(subtaskId);
            if (keyRemainingWeight < subtaskRemainingWeight) {
                // assign the remaining weight of the key to the current subtask
                subtaskWeights.add(keyRemainingWeight);
                subtaskRemainingWeight -= keyRemainingWeight;
                keyRemainingWeight = 0L;
            } else {
                // filled up the current subtask
                long assignedWeight = subtaskRemainingWeight;
                keyRemainingWeight -= subtaskRemainingWeight;

                subtaskWeights.add(assignedWeight);
                // move on to the next subtask
                subtaskId += 1;
                subtaskRemainingWeight = targetWeightPerSubtask;
            }

            checkState(
                    assignedSubtasks.size() == subtaskWeights.size(),
                    "List size mismatch: assigned subtasks = %s, subtask weights = %s",
                    assignedSubtasks,
                    subtaskWeights);

            if (keyRemainingWeight == 0) {
                // finishing up the assignment for the current key
                PartitionAssignment keyAssignment =
                        hasBucketKey
                                ? new WeightedBucketIdAssignment(
                                        assignedSubtasks,
                                        subtaskWeights,
                                        bucketNum,
                                        bucketKeyEncoder,
                                        bucketingFunction,
                                        random)
                                : new WeightedRandomAssignment(
                                        assignedSubtasks, subtaskWeights, random);
                assignmentMap.put(currentKey, keyAssignment);
                assignedSubtasks = new ArrayList<>();
                subtaskWeights = new ArrayList<>();
                currentKey = null;
            }
        }

        LOG.debug("Assignment map: {}", assignmentMap);
        return assignmentMap;
    }

    @Override
    public String toString() {
        return "PARTITION_DYNAMIC";
    }

    private class MapPartitioner {

        private final Map<String, PartitionAssignment> assignments;

        public MapPartitioner() {
            this(new HashMap<>());
        }

        public MapPartitioner(Map<String, PartitionAssignment> assignments) {
            this.assignments = assignments;
        }

        int select(String partitionName, InternalRow row, int numChannels) {
            PartitionAssignment keyAssignment = assignments.get(partitionName);
            if (keyAssignment == null) {
                int defaultSubtaskCount = Math.min(bucketNum, numChannels);
                LOG.debug(
                        "Encountered new partition: {}.  choose {} subtasks for it.",
                        partitionName,
                        defaultSubtaskCount);
                int randomStart = Math.abs(partitionName.hashCode()) % numChannels;
                List<Integer> assignedSubtasks = new ArrayList<>(defaultSubtaskCount);
                List<Long> subtaskWeights = new ArrayList<>(defaultSubtaskCount);
                for (int i = 0; i < defaultSubtaskCount; i++) {
                    assignedSubtasks.add((randomStart + i) % numChannels);
                    subtaskWeights.add(1L);
                }
                keyAssignment =
                        hasBucketKey()
                                ? new WeightedBucketIdAssignment(
                                        assignedSubtasks,
                                        subtaskWeights,
                                        bucketNum,
                                        bucketKeyEncoder,
                                        bucketingFunction,
                                        random)
                                : new WeightedRandomAssignment(
                                        assignedSubtasks, subtaskWeights, random);
                assignments.put(partitionName, keyAssignment);
            }

            return keyAssignment.select(row);
        }
    }

    @VisibleForTesting
    void setRandom(Random random) {
        this.random = random;
    }
}
