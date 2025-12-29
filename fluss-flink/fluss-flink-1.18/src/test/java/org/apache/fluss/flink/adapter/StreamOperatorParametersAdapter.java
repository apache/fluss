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

package org.apache.fluss.flink.adapter;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.function.Supplier;

/**
 * Adapter for {@link StreamOperatorParameters} because the constructor is compatibility in flink
 * 1.18 and 1.19. However, this constructor only used in test.
 *
 * <p>TODO: remove this class when no longer support flink 1.18 and 1.19.
 */
public class StreamOperatorParametersAdapter {

    public static <OUT> StreamOperatorParameters<OUT> create(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output,
            Supplier<ProcessingTimeService> processingTimeServiceFactory,
            OperatorEventDispatcher operatorEventDispatcher,
            MailboxExecutor mailboxExecutor) {
        return new StreamOperatorParameters<>(
                containingTask,
                config,
                output,
                processingTimeServiceFactory,
                operatorEventDispatcher);
    }
}
