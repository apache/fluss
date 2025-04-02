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

package com.alibaba.fluss.flink.source.reader;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.source.emitter.FlinkRecordEmitter;
import com.alibaba.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.table.data.RowData;
import org.jetbrains.annotations.Nullable;

/** Test for {@link FlinkSourceReader}. */
class FlinkSourceReaderTest extends SourceReaderTestBase {

    @Override
    public SourceReader<RowData, SourceSplitBase> sourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue,
            Configuration flussConfig,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            @Nullable int[] projectedFields,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics,
            FlinkRecordEmitter<RowData> recordEmitter) {
        return new FlinkSourceReader<>(
                flussConfig,
                tablePath,
                sourceOutputType,
                context,
                null,
                new FlinkSourceReaderMetrics(context.metricGroup()),
                recordEmitter);
    }
}
