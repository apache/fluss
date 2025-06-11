/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.committer;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/** The factory to create {@link TieringCommitOperator}. */
public class TieringCommitOperatorFactory<WriteResult, Committable>
        extends AbstractStreamOperatorFactory<CommittableMessage<Committable>> {

    private final Configuration flussConfig;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;

    public TieringCommitOperatorFactory(
            Configuration flussConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.flussConfig = flussConfig;
        this.lakeTieringFactory = lakeTieringFactory;
    }

    @Override
    public <T extends StreamOperator<CommittableMessage<Committable>>> T createStreamOperator(
            StreamOperatorParameters<CommittableMessage<Committable>> parameters) {

        TieringCommitOperator<WriteResult, Committable> commitOperator =
                new TieringCommitOperator<>(parameters, flussConfig, lakeTieringFactory);

        @SuppressWarnings("unchecked")
        final T castedOperator = (T) commitOperator;

        return castedOperator;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return TieringCommitOperator.class;
    }
}
