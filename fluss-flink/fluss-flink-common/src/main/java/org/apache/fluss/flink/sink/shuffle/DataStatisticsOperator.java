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
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

import static org.apache.fluss.flink.adapter.RuntimeContextAdapter.getIndexOfThisSubtask;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Data statistics operator which collects local data statistics and sends them to coordinator for
 * global data statics, then sends global data statics to partitioner.
 *
 * @param <InputT>
 */
@Internal
public class DataStatisticsOperator<InputT>
        extends AbstractStreamOperator<StatisticsOrRecord<InputT>>
        implements OneInputStreamOperator<InputT, StatisticsOrRecord<InputT>>,
                OperatorEventHandler {

    private static final long serialVersionUID = 1L;

    private final String operatorName;
    private final RowType rowType;
    private final List<String> partitionKeys;
    private final FlussSerializationSchema<InputT> flussSerializationSchema;
    private final OperatorEventGateway operatorEventGateway;

    private transient int subtaskIndex;
    private transient volatile DataStatistics localStatistics;
    private transient PartitionGetter partitionGetter;
    private transient TypeSerializer<DataStatistics> statisticsSerializer;

    DataStatisticsOperator(
            StreamOperatorParameters<StatisticsOrRecord<InputT>> parameters,
            String operatorName,
            RowType rowType,
            List<String> partitionKeys,
            OperatorEventGateway operatorEventGateway,
            FlussSerializationSchema<InputT> flussSerializationSchema) {
        super();
        checkArgument(
                partitionKeys != null && !partitionKeys.isEmpty(),
                "Partition keys cannot be empty.");
        this.operatorName = operatorName;
        this.operatorEventGateway = operatorEventGateway;
        this.flussSerializationSchema = flussSerializationSchema;
        this.rowType = rowType;
        this.partitionKeys = partitionKeys;
        this.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
    }

    @Override
    public void open() throws Exception {
        this.partitionGetter = new PartitionGetter(rowType, partitionKeys);
        this.statisticsSerializer = new DataStatisticsSerializer();
        try {
            this.flussSerializationSchema.open(new SerializerInitContextImpl(rowType));
        } catch (Exception e) {
            throw new FlussRuntimeException(e);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        this.subtaskIndex = getIndexOfThisSubtask(getRuntimeContext());
        this.localStatistics = StatisticsUtil.createDataStatistics();
    }

    @Override
    public void handleOperatorEvent(OperatorEvent event) {
        checkArgument(
                event instanceof StatisticsEvent,
                String.format(
                        "Operator %s subtask %s received unexpected operator event %s",
                        operatorName, subtaskIndex, event.getClass()));
        StatisticsEvent statisticsEvent = (StatisticsEvent) event;
        LOG.debug(
                "Operator {} subtask {} received global data event from coordinator checkpoint {}",
                operatorName,
                subtaskIndex,
                statisticsEvent.getCheckpointId());
        DataStatistics globalStatistics =
                StatisticsUtil.deserializeDataStatistics(
                        statisticsEvent.getStatisticsBytes(), statisticsSerializer);
        LOG.debug("Global data statistics: {}", globalStatistics);
        if (globalStatistics != null) {
            output.collect(new StreamRecord<>(StatisticsOrRecord.fromStatistics(globalStatistics)));
        }
    }

    @Override
    public void processElement(StreamRecord<InputT> streamRecord) throws Exception {
        // collect data statistics
        InputT record = streamRecord.getValue();
        RowWithOp rowWithOp = flussSerializationSchema.serialize(record);
        InternalRow row = rowWithOp.getRow();
        String partition = partitionGetter.getPartition(row);
        // if estimated size is null, use row count rather than bytes size as weight.
        long weight =
                rowWithOp.getEstimatedSizeInBytes() != null
                        ? rowWithOp.getEstimatedSizeInBytes()
                        : 1;
        localStatistics.add(partition, weight);
        output.collect(new StreamRecord<>(StatisticsOrRecord.fromRecord(record)));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) {
        long checkpointId = context.getCheckpointId();
        LOG.debug(
                "Operator {} Subtask {} sending local statistics to coordinator for checkpoint {}",
                operatorName,
                subtaskIndex,
                checkpointId);
        operatorEventGateway.sendEventToCoordinator(
                StatisticsEvent.createStatisticsEvent(
                        checkpointId, localStatistics, statisticsSerializer));

        // Recreate the local statistics
        localStatistics = StatisticsUtil.createDataStatistics();
    }

    @VisibleForTesting
    public DataStatistics getLocalStatistics() {
        return localStatistics;
    }
}
