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

package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.sink.AbstractSink.SinkWriterBuilder;
import com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.MergeEngineType;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** A Flink {@link DynamicTableSink}. */
public class FlinkTableSink extends AbstractTableSink {

    public FlinkTableSink(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            int[] primaryKeyIndexes,
            List<String> partitionKeys,
            boolean streaming,
            @Nullable MergeEngineType mergeEngineType,
            @Nullable DataLakeFormat lakeFormat,
            boolean ignoreDelete,
            int numBucket,
            List<String> bucketKeys,
            boolean shuffleByBucketId) {
        super(
                tablePath,
                flussConfig,
                tableRowType,
                primaryKeyIndexes,
                partitionKeys,
                streaming,
                mergeEngineType,
                lakeFormat,
                ignoreDelete,
                numBucket,
                bucketKeys,
                shuffleByBucketId);
    }

    @Override
    public Sink<RowData> sink(SinkWriterBuilder<? extends FlinkSinkWriter> sinkWriterBuilder) {
        return new FlinkSink(sinkWriterBuilder);
    }

    @Override
    public DynamicTableSink copy() {
        FlinkTableSink sink =
                new FlinkTableSink(
                        tablePath,
                        flussConfig,
                        tableRowType,
                        primaryKeyIndexes,
                        partitionKeys,
                        streaming,
                        mergeEngineType,
                        lakeFormat,
                        ignoreDelete,
                        numBucket,
                        bucketKeys,
                        shuffleByBucketId);
        sink.appliedUpdates = appliedUpdates;
        sink.deleteRow = deleteRow;
        return sink;
    }
}
