/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.sink;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.sink.writer.AppendSinkWriter;
import com.alibaba.fluss.connector.flink.sink.writer.FlinkSinkWriter;
import com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Test for {@link com.alibaba.fluss.connector.flink.sink.writer.FlinkSinkWriter}. */
public class FlinkSinkWriterTest extends FlinkTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"", "1"})
    void testSinkMetrics(String clientId) throws Exception {
        TablePath tablePath = TablePath.of("test_sink_function_db", "test_sink_function_table");
        admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, false);
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", com.alibaba.fluss.types.DataTypes.INT())
                                        .column("name", com.alibaba.fluss.types.DataTypes.STRING())
                                        .build())
                        .build();
        createTable(tablePath, tableDescriptor);
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        flussConf.set(ConfigOptions.CLIENT_ID, clientId);

        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new RowType.RowField("id", DataTypes.INT().getLogicalType()),
                                new RowType.RowField("name", DataTypes.STRING().getLogicalType())));
        FlinkSinkWriter flinkSinkWriter =
                new AppendSinkWriter(tablePath, flussConf, rowType, false);

        InterceptingOperatorMetricGroup interceptingOperatorMetricGroup =
                new InterceptingOperatorMetricGroup();
        MockStreamingRuntimeContext mockStreamingRuntimeContext =
                new MockStreamingRuntimeContext(false, 1, 0) {
                    @Override
                    public OperatorMetricGroup getMetricGroup() {
                        return interceptingOperatorMetricGroup;
                    }
                };
        MockWriterInitContext mockWriterInitContext =
                new MockWriterInitContext(mockStreamingRuntimeContext);

        flinkSinkWriter.initialize(mockWriterInitContext.metricGroup());
        flinkSinkWriter.write(
                GenericRowData.of(1, StringData.fromString("a")), new MockSinkWriterContext());
        flinkSinkWriter.flush(false);

        Metric currentSendTime = interceptingOperatorMetricGroup.get(MetricNames.CURRENT_SEND_TIME);
        assertThat(currentSendTime).isInstanceOf(Gauge.class);
        assertThat(((Gauge<Long>) currentSendTime).getValue()).isGreaterThan(0);

        Metric numRecordSend = interceptingOperatorMetricGroup.get(MetricNames.NUM_RECORDS_SEND);
        assertThat(numRecordSend).isInstanceOf(Counter.class);
        assertThat(((Counter) numRecordSend).getCount()).isGreaterThan(0);

        flinkSinkWriter.close();
    }

    static class MockSinkWriterContext implements SinkWriter.Context {
        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return 0L;
        }
    }

    static class MockWriterInitContext implements WriterInitContext {

        private static final String UNEXPECTED_METHOD_CALL_MESSAGE =
                "Unexpected method call. Expected that this method will never be called by a test case.";

        private final MockStreamingRuntimeContext mockStreamingRuntimeContext;

        public MockWriterInitContext(MockStreamingRuntimeContext mockStreamingRuntimeContext) {
            this.mockStreamingRuntimeContext = mockStreamingRuntimeContext;
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
            return InternalSinkWriterMetricGroup.wrap(mockStreamingRuntimeContext.getMetricGroup());
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
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

        public JobInfo getJobInfo() {
            fail(UNEXPECTED_METHOD_CALL_MESSAGE);
            return null;
        }

        public TaskInfo getTaskInfo() {
            fail(UNEXPECTED_METHOD_CALL_MESSAGE);
            return null;
        }

        public int getSubtaskId() {
            fail(UNEXPECTED_METHOD_CALL_MESSAGE);
            return 0;
        }

        public int getNumberOfParallelSubtasks() {
            fail(UNEXPECTED_METHOD_CALL_MESSAGE);
            return 0;
        }

        public int getAttemptNumber() {
            fail(UNEXPECTED_METHOD_CALL_MESSAGE);
            return 0;
        }

        public JobID getJobId() {
            fail(UNEXPECTED_METHOD_CALL_MESSAGE);
            return null;
        }
    }
}
