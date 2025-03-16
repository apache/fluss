/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.sink;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.fail;

/** A mock implementation of {@link WriterInitContext} for testing purposes. */
public class MockWriterInitContext implements WriterInitContext {
    private static final String UNEXPECTED_METHOD_CALL_MESSAGE =
            "Unexpected method call. Expected that this method will never be called by a test case.";

    private final RuntimeContext runtimeContext;

    public MockWriterInitContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        fail(UNEXPECTED_METHOD_CALL_MESSAGE);
        return null;
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        fail(UNEXPECTED_METHOD_CALL_MESSAGE);
        return null;
    }

    @Override
    public ProcessingTimeService getProcessingTimeService() {
        fail(UNEXPECTED_METHOD_CALL_MESSAGE);
        return null;
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return InternalSinkWriterMetricGroup.wrap(runtimeContext.getMetricGroup());
    }

    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        fail(UNEXPECTED_METHOD_CALL_MESSAGE);
        return null;
    }

    @Override
    public boolean isObjectReuseEnabled() {
        fail(UNEXPECTED_METHOD_CALL_MESSAGE);
        return false;
    }

    @Override
    public <IN> TypeSerializer<IN> createInputSerializer() {
        fail(UNEXPECTED_METHOD_CALL_MESSAGE);
        return null;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        fail(UNEXPECTED_METHOD_CALL_MESSAGE);
        return OptionalLong.empty();
    }

    @Override
    public JobInfo getJobInfo() {
        fail(UNEXPECTED_METHOD_CALL_MESSAGE);
        return null;
    }

    @Override
    public TaskInfo getTaskInfo() {
        fail(UNEXPECTED_METHOD_CALL_MESSAGE);
        return null;
    }
}
