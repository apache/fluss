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
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.types.RowType;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.util.List;

/**
 * Flink operator factory for DataStatisticsOperator.
 *
 * @param <InputT>
 */
@Internal
public class DataStatisticsOperatorFactory<InputT>
        extends AbstractStreamOperatorFactory<StatisticsOrRecord<InputT>>
        implements CoordinatedOperatorFactory<StatisticsOrRecord<InputT>>,
                OneInputStreamOperatorFactory<InputT, StatisticsOrRecord<InputT>> {

    private final RowType rowType;
    private final List<String> partitionKeys;
    private final FlussSerializationSchema<InputT> flussSerializationSchema;

    public DataStatisticsOperatorFactory(
            RowType rowType,
            List<String> partitionKeys,
            FlussSerializationSchema<InputT> flussSerializationSchema) {
        this.rowType = rowType;
        this.partitionKeys = partitionKeys;
        this.flussSerializationSchema = flussSerializationSchema;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new DataStatisticsCoordinatorProvider(operatorName, operatorID);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends StreamOperator<StatisticsOrRecord<InputT>>> T createStreamOperator(
            StreamOperatorParameters<StatisticsOrRecord<InputT>> parameters) {
        OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        String operatorName = parameters.getStreamConfig().getOperatorName();
        OperatorEventGateway gateway =
                parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);

        DataStatisticsOperator<InputT> rangeStatisticsOperator =
                new DataStatisticsOperator<>(
                        parameters,
                        operatorName,
                        rowType,
                        partitionKeys,
                        gateway,
                        flussSerializationSchema);
        parameters
                .getOperatorEventDispatcher()
                .registerEventHandler(operatorId, rangeStatisticsOperator);
        return (T) rangeStatisticsOperator;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<DataStatisticsOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return DataStatisticsOperator.class;
    }
}
