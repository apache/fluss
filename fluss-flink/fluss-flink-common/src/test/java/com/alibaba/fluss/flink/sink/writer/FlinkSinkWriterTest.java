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

package com.alibaba.fluss.flink.sink.writer;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.NetworkException;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.sink.serializer.SerializerInitContextImpl;
import com.alibaba.fluss.flink.utils.FlinkTestBase;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.function.BiConsumer;

import static com.alibaba.fluss.flink.utils.FlinkConversions.toFlussRowType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter}. */
public class FlinkSinkWriterTest extends FlinkTestBase {
    private static final String DEFAULT_SINK_DB = "test-sink-db";

    private static final TablePath DEFAULT_SINK_TABLE_PATH =
            TablePath.of(DEFAULT_SINK_DB, "test-sink-table");

    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("id", com.alibaba.fluss.types.DataTypes.INT())
                                    .column("name", com.alibaba.fluss.types.DataTypes.CHAR(10))
                                    .build())
                    .build();

    @ParameterizedTest
    @ValueSource(strings = {"", "1"})
    void testSinkMetrics(String clientId) throws Exception {
        admin.createDatabase(
                DEFAULT_SINK_TABLE_PATH.getDatabaseName(), DatabaseDescriptor.EMPTY, true);
        createTable(DEFAULT_SINK_TABLE_PATH, TABLE_DESCRIPTOR);

        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        flussConf.set(ConfigOptions.CLIENT_ID, clientId);

        InterceptingOperatorMetricGroup interceptingOperatorMetricGroup =
                new InterceptingOperatorMetricGroup();
        MockWriterInitContext mockWriterInitContext =
                new MockWriterInitContext(interceptingOperatorMetricGroup);
        FlinkSinkWriter<RowData> flinkSinkWriter =
                createSinkWriter(flussConf, mockWriterInitContext.getMailboxExecutor());

        flinkSinkWriter.initialize(mockWriterInitContext.metricGroup());
        flinkSinkWriter.write(
                GenericRowData.of(1, StringData.fromString("a")), new MockSinkWriterContext());
        flinkSinkWriter.flush(false);

        Metric currentSendTime = interceptingOperatorMetricGroup.get(MetricNames.CURRENT_SEND_TIME);
        assertThat(currentSendTime).isInstanceOf(Gauge.class);
        // the default send latency is -1, so check it is >= 0, as the latency maybe very small 0ms
        assertThat(((Gauge<Long>) currentSendTime).getValue()).isGreaterThanOrEqualTo(0);

        Metric numRecordSend = interceptingOperatorMetricGroup.get(MetricNames.NUM_RECORDS_SEND);
        assertThat(numRecordSend).isInstanceOf(Counter.class);
        assertThat(((Counter) numRecordSend).getCount()).isGreaterThan(0);

        flinkSinkWriter.close();
    }

    @Test
    void testWriteExceptionWhenFlussUnavailable() throws Exception {
        testExceptionWhenFlussUnavailable(
                (writer, mailboxExecutor) -> {
                    // Flush client here to make sure last write is finished but not check
                    // exception.
                    writer.getTableWriter().flush();
                    assertThatThrownBy(
                                    () ->
                                            writer.write(
                                                    GenericRowData.of(
                                                            2, StringData.fromString("b")),
                                                    new MockSinkWriterContext()))
                            .hasRootCauseExactlyInstanceOf(NetworkException.class);
                });
    }

    @Test
    void testFlushExceptionWhenFlussUnavailable() throws Exception {
        testExceptionWhenFlussUnavailable(
                (writer, mailboxExecutor) -> {
                    // Flush SinkWriter here to make sure last write finish and also check
                    // exception.
                    assertThatThrownBy(() -> writer.flush(true))
                            .hasRootCauseExactlyInstanceOf(NetworkException.class);
                });
    }

    @Test
    void testCloseExceptionWhenFlussUnavailable() throws Exception {
        testExceptionWhenFlussUnavailable(
                (writer, mailboxExecutor) -> {
                    // Flush client here to make sure last write is finished but not check
                    // exception.
                    writer.getTableWriter().flush();
                    assertThatThrownBy(writer::close)
                            .hasRootCauseExactlyInstanceOf(NetworkException.class);
                });
    }

    @Test
    void testMailBoxExceptionWhenFlussUnavailable() throws Exception {
        testExceptionWhenFlussUnavailable(
                (writer, mailboxExecutor) -> {
                    // Flush client here to make sure last write is finished but not check
                    // exception.
                    writer.getTableWriter().flush();
                    assertThatThrownBy(
                                    () -> {
                                        while (mailboxExecutor.tryYield()) {
                                            // execute all mails
                                        }
                                    })
                            .hasRootCauseExactlyInstanceOf(NetworkException.class);
                });
    }

    private void testExceptionWhenFlussUnavailable(
            BiConsumer<FlinkSinkWriter<RowData>, MailboxExecutor> actionAfterFlussUnavailable)
            throws Exception {
        FlussClusterExtension flussClusterExtension = FlussClusterExtension.builder().build();
        try {
            flussClusterExtension.start();

            // prepare table
            Configuration clientConfig = flussClusterExtension.getClientConfig();
            clientConfig.set(ConfigOptions.CLIENT_WRITER_RETRIES, 0);
            clientConfig.set(ConfigOptions.CLIENT_WRITER_ENABLE_IDEMPOTENCE, false);
            try (Connection connection = ConnectionFactory.createConnection(clientConfig);
                    Admin admin = connection.getAdmin()) {
                admin.createDatabase(
                                DEFAULT_SINK_TABLE_PATH.getDatabaseName(),
                                DatabaseDescriptor.EMPTY,
                                true)
                        .get();
                admin.createTable(DEFAULT_SINK_TABLE_PATH, TABLE_DESCRIPTOR, true).get();
            }

            MockWriterInitContext mockWriterInitContext =
                    new MockWriterInitContext(new InterceptingOperatorMetricGroup());
            // test fluss unavailable.
            try (FlinkSinkWriter<RowData> writer =
                    createSinkWriter(clientConfig, mockWriterInitContext.getMailboxExecutor())) {
                writer.initialize(mockWriterInitContext.metricGroup());
                flussClusterExtension.close();
                writer.write(
                        GenericRowData.of(1, StringData.fromString("a")),
                        new MockSinkWriterContext());
                actionAfterFlussUnavailable.accept(
                        writer, mockWriterInitContext.getMailboxExecutor());
            }
        } finally {
            flussClusterExtension.close();
        }
    }

    private FlinkSinkWriter<RowData> createSinkWriter(
            Configuration configuration, MailboxExecutor mailboxExecutor) throws Exception {
        RowType tableRowType =
                RowType.of(
                        new LogicalType[] {new IntType(), new CharType(10)},
                        new String[] {"id", "name"});
        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(true, false);
        serializationSchema.open(new SerializerInitContextImpl(toFlussRowType(tableRowType)));
        return new AppendSinkWriter<>(
                DEFAULT_SINK_TABLE_PATH,
                configuration,
                tableRowType,
                mailboxExecutor,
                serializationSchema);
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
}
