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

package com.alibaba.fluss.lakehouse.paimon.testutils;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.sink.FlinkTableSink;
import com.alibaba.fluss.connector.flink.utils.FlinkConversions;
import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** A sink to write {@link MultiplexCdcRecord} to fluss for testing purpose. */
public class TestingDatabaseSyncSink extends RichSinkFunction<MultiplexCdcRecord>
        implements CheckpointedFunction, Serializable {

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final String sinkDataBase;
    private org.apache.flink.configuration.Configuration flinkConfig;

    private transient Connection connection;
    private transient Admin admin;
    private transient Map<TablePath, SinkFunction<RowData>> sinkByTablePath;

    public TestingDatabaseSyncSink(String sinkDataBase, Configuration flussConfig) {
        this.sinkDataBase = sinkDataBase;
        this.flussConfig = flussConfig;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration config) {
        this.connection = ConnectionFactory.createConnection(flussConfig);
        this.admin = connection.getAdmin();
        this.flinkConfig = config;
        this.sinkByTablePath = new HashMap<>();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {
        // do nothing
    }

    @Override
    public void invoke(MultiplexCdcRecord record, SinkFunction.Context context) throws Exception {
        TablePath tablePath = record.getTablePath();
        SinkFunction<RowData> sinkFunction = sinkByTablePath.get(tablePath);
        if (sinkFunction == null) {
            TableDescriptor tableDescriptor =
                    admin.getTableInfo(tablePath).get().getTableDescriptor();
            RowType rowType = tableDescriptor.getSchema().toRowType();

            FlinkTableSink flinkTableSink =
                    new FlinkTableSink(
                            // write to the sink database
                            new TablePath(sinkDataBase, tablePath.getTableName()),
                            flussConfig,
                            FlinkConversions.toFlinkRowType(rowType),
                            tableDescriptor.getSchema().getPrimaryKeyIndexes(),
                            true,
                            null,
                            false);

            sinkFunction =
                    ((SinkFunctionProvider)
                                    flinkTableSink.getSinkRuntimeProvider(
                                            new SinkRuntimeProviderContext(false)))
                            .createSinkFunction();

            if (sinkFunction instanceof RichSinkFunction) {
                RichSinkFunction<RowData> richSinkFunction =
                        (RichSinkFunction<RowData>) sinkFunction;
                richSinkFunction.setRuntimeContext(getRuntimeContext());
                richSinkFunction.open(flinkConfig);
            }
            sinkByTablePath.put(tablePath, sinkFunction);
        }

        sinkFunction.invoke(record.getCdcRecord().getRowData(), context);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        for (SinkFunction<RowData> sinkFunction : sinkByTablePath.values()) {
            if (sinkFunction instanceof CheckpointedFunction) {
                ((CheckpointedFunction) sinkFunction).snapshotState(functionSnapshotContext);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }

        if (sinkByTablePath != null) {
            for (SinkFunction<RowData> sinkFunction : sinkByTablePath.values()) {
                if (sinkFunction instanceof RichSinkFunction) {
                    ((RichSinkFunction<RowData>) sinkFunction).close();
                }
            }
        }
    }
}
