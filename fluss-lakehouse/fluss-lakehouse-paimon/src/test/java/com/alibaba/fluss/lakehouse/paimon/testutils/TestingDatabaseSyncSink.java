/*
 *  Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.lakehouse.paimon.testutils;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.sink.FlinkTableSink;
import com.alibaba.fluss.connector.flink.utils.FlinkConversions;
import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/** A sink to write {@link MultiplexCdcRecord} to fluss for testing purpose. */
public class TestingDatabaseSyncSink implements Sink<MultiplexCdcRecord> {

    private static final long serialVersionUID = 1L;

    private final String sinkDataBase;
    private final Configuration flussConfig;

    public TestingDatabaseSyncSink(String sinkDataBase, Configuration flussConfig) {
        this.sinkDataBase = sinkDataBase;
        this.flussConfig = flussConfig;
    }

    @Override
    public SinkWriter<MultiplexCdcRecord> createWriter(WriterInitContext context)
            throws IOException {
        return new TestingDatabaseSyncSinkWriter(sinkDataBase, flussConfig, context);
    }

    @Deprecated
    @Override
    public SinkWriter<MultiplexCdcRecord> createWriter(InitContext context) throws IOException {
        return new TestingDatabaseSyncSinkWriter(sinkDataBase, flussConfig, context);
    }

    /** A sink writer to write {@link MultiplexCdcRecord} to fluss for testing purpose. */
    public static class TestingDatabaseSyncSinkWriter implements SinkWriter<MultiplexCdcRecord> {

        private final String sinkDataBase;
        private final Configuration flussConfig;

        private WriterInitContext writerInitContext;
        @Deprecated private InitContext initContext;

        private transient Connection connection;
        private transient Admin admin;
        private transient Map<TablePath, SinkWriter<RowData>> sinkWriterByTablePath;

        private TestingDatabaseSyncSinkWriter(String sinkDataBase, Configuration flussConfig) {
            this.sinkDataBase = sinkDataBase;
            this.flussConfig = flussConfig;
            this.connection = ConnectionFactory.createConnection(flussConfig);
            this.admin = connection.getAdmin();
            this.sinkWriterByTablePath = new HashMap<>();
        }

        public TestingDatabaseSyncSinkWriter(
                String sinkDataBase, Configuration flussConfig, WriterInitContext writerInitContext)
                throws IOException {
            this(sinkDataBase, flussConfig);
            this.writerInitContext = writerInitContext;
            this.initContext = null;
        }

        @Deprecated
        public TestingDatabaseSyncSinkWriter(
                String sinkDataBase, Configuration flussConfig, InitContext context)
                throws IOException {
            this(sinkDataBase, flussConfig);
            this.writerInitContext = null;
            this.initContext = context;
        }

        @Override
        public void write(MultiplexCdcRecord record, Context context)
                throws IOException, InterruptedException {
            TablePath tablePath = record.getTablePath();
            SinkWriter<RowData> sinkWriter = sinkWriterByTablePath.get(tablePath);

            if (sinkWriter == null) {
                TableInfo tableInfo;
                try {
                    tableInfo = admin.getTableInfo(tablePath).get();
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }

                FlinkTableSink flinkTableSink =
                        new FlinkTableSink(
                                // write to the sink database
                                new TablePath(sinkDataBase, tablePath.getTableName()),
                                flussConfig,
                                FlinkConversions.toFlinkRowType(tableInfo.getRowType()),
                                tableInfo.getSchema().getPrimaryKeyIndexes(),
                                true,
                                null,
                                false);

                Sink<RowData> sink =
                        ((SinkV2Provider)
                                        flinkTableSink.getSinkRuntimeProvider(
                                                new SinkRuntimeProviderContext(false)))
                                .createSink();

                if (writerInitContext != null) {
                    sinkWriter = sink.createWriter(writerInitContext);
                } else if (initContext != null) {
                    sinkWriter = sink.createWriter(initContext);
                } else {
                    throw new IllegalStateException(
                            "Neither writer init context nor init context was set.");
                }

                sinkWriterByTablePath.put(tablePath, sinkWriter);
            }

            sinkWriter.write(record.getCdcRecord().getRowData(), context);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            for (SinkWriter<RowData> sinkWriter : sinkWriterByTablePath.values()) {
                sinkWriter.flush(endOfInput);
            }
        }

        @Override
        public void close() throws Exception {
            if (admin != null) {
                admin.close();
            }

            if (connection != null) {
                connection.close();
            }

            if (sinkWriterByTablePath != null) {
                for (SinkWriter<RowData> sinkWriter : sinkWriterByTablePath.values()) {
                    sinkWriter.close();
                }
            }
        }
    }
}
