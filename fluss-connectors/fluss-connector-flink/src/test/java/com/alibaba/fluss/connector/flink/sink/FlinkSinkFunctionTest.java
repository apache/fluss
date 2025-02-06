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
import com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkSinkFunction}. */
public class FlinkSinkFunctionTest extends FlinkTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"", "1"})
    void testSinkMetrics(String clientId) throws Exception {
        TablePath tablePath = TablePath.of("test_sink_function_db", "test_sink_function_table");
        admin.createDatabase(tablePath.getDatabaseName(), false);
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
        FlinkSinkFunction flinkSinkFunction =
                new AppendSinkFunction(tablePath, flussConf, rowType, false);
        InterceptingOperatorMetricGroup interceptingOperatorMetricGroup =
                new InterceptingOperatorMetricGroup();
        MockStreamingRuntimeContext mockStreamingRuntimeContext =
                new MockStreamingRuntimeContext(false, 1, 0) {
                    @Override
                    public OperatorMetricGroup getMetricGroup() {
                        return interceptingOperatorMetricGroup;
                    }
                };
        flinkSinkFunction.setRuntimeContext(mockStreamingRuntimeContext);
        flinkSinkFunction.open(new org.apache.flink.configuration.Configuration());
        flinkSinkFunction.invoke(
                GenericRowData.of(1, StringData.fromString("a")), new MockSinkContext());
        flinkSinkFunction.flush();

        Metric currentSendTime = interceptingOperatorMetricGroup.get(MetricNames.CURRENT_SEND_TIME);
        assertThat(currentSendTime).isInstanceOf(Gauge.class);
        assertThat(((Gauge<Long>) currentSendTime).getValue()).isGreaterThan(0);

        Metric numRecordSend = interceptingOperatorMetricGroup.get(MetricNames.NUM_RECORDS_SEND);
        assertThat(numRecordSend).isInstanceOf(Counter.class);
        assertThat(((Counter) numRecordSend).getCount()).isGreaterThan(0);

        flinkSinkFunction.close();
    }

    static class MockSinkContext implements SinkFunction.Context {
        @Override
        public long currentProcessingTime() {
            return 0;
        }

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
