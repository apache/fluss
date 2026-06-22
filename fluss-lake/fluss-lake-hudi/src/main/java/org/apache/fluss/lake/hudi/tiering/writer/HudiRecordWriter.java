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

package org.apache.fluss.lake.hudi.tiering.writer;

import org.apache.fluss.lake.hudi.tiering.HudiWriteTableInfo;
import org.apache.fluss.lake.hudi.tiering.RecordWriter;
import org.apache.fluss.lake.hudi.utils.meta.CkpMetadata;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.LogRecord;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.sink.bulk.RowDataKeyGen;

/** Hudi {@link RecordWriter} implementation. */
public class HudiRecordWriter extends RecordWriter {

    private final TableInfo tableInfo;
    private final Configuration config;
    private final RowDataKeyGen keyGen;

    public HudiRecordWriter(
            WriterInitContext writerInitContext,
            TableInfo tableInfo,
            HudiWriteTableInfo hudiTableInfo,
            CkpMetadata ckpMetadata) {
        super(writerInitContext, hudiTableInfo, ckpMetadata);
        this.tableInfo = tableInfo;
        this.config = hudiTableInfo.getFlinkConfig();
        this.keyGen =
                RowDataKeyGen.instance(hudiTableInfo.getFlinkConfig(), hudiTableInfo.getRowType());
    }

    @Override
    public void write(LogRecord record) throws Exception {
        flussRecordAsHudiRecord.setFlussRecord(record);
        HoodieFlinkInternalRow internalRow =
                new HoodieFlinkInternalRow(
                        keyGen.getRecordKey(flussRecordAsHudiRecord),
                        keyGen.getPartitionPath(flussRecordAsHudiRecord),
                        HoodieOperation.fromValue(
                                        flussRecordAsHudiRecord.getRowKind().toByteValue())
                                .getName(),
                        flussRecordAsHudiRecord);
        setRecordLocation(internalRow, config);
        recordWriteBuffer.bufferRecord(internalRow);
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }
}
