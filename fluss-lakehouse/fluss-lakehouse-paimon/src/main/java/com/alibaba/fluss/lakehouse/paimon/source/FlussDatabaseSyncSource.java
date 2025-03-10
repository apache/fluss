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

package com.alibaba.fluss.lakehouse.paimon.source;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;
import com.alibaba.fluss.lakehouse.paimon.source.enumerator.FlinkSourceEnumerator;
import com.alibaba.fluss.lakehouse.paimon.source.metrics.FlinkSourceReaderMetrics;
import com.alibaba.fluss.lakehouse.paimon.source.reader.FlinkSourceReader;
import com.alibaba.fluss.lakehouse.paimon.source.reader.MultiplexCdcRecordAndPos;
import com.alibaba.fluss.lakehouse.paimon.source.split.SourceSplitBase;
import com.alibaba.fluss.lakehouse.paimon.source.split.SourceSplitSerializer;
import com.alibaba.fluss.lakehouse.paimon.source.state.FlussSourceEnumeratorStateSerializer;
import com.alibaba.fluss.lakehouse.paimon.source.state.SourceEnumeratorState;
import com.alibaba.fluss.metadata.TableInfo;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/** A flink source for syncing the fluss database to somewhere. */
// todo: implements ResultTypeQueryable
public class FlussDatabaseSyncSource
        implements Source<MultiplexCdcRecord, SourceSplitBase, SourceEnumeratorState> {

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final Filter<String> databaseFilter;
    private final Filter<TableInfo> tableFilter;
    private final NewTablesAddedListener newTablesAddedListener;

    public FlussDatabaseSyncSource(
            Configuration flussConfig,
            Filter<String> databaseFilter,
            Filter<TableInfo> tableFilter,
            NewTablesAddedListener tableAddedListener) {
        this.flussConfig = flussConfig;
        this.databaseFilter = databaseFilter;
        this.tableFilter = tableFilter;
        this.newTablesAddedListener = tableAddedListener;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<SourceSplitBase> splitEnumeratorContext) throws Exception {
        return new FlinkSourceEnumerator(
                flussConfig,
                databaseFilter,
                tableFilter,
                newTablesAddedListener,
                splitEnumeratorContext);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<SourceSplitBase> splitEnumeratorContext,
            SourceEnumeratorState sourceEnumeratorState) {
        return new FlinkSourceEnumerator(
                flussConfig,
                databaseFilter,
                tableFilter,
                newTablesAddedListener,
                splitEnumeratorContext,
                sourceEnumeratorState.getAssignedTables(),
                sourceEnumeratorState.getAssignedPartitions(),
                sourceEnumeratorState.getAssignedBuckets());
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitBase> getSplitSerializer() {
        return SourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<SourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return FlussSourceEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public SourceReader<MultiplexCdcRecord, SourceSplitBase> createReader(
            SourceReaderContext sourceReaderContext) {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<MultiplexCdcRecordAndPos>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        FlinkSourceReaderMetrics flinkSourceReaderMetrics =
                new FlinkSourceReaderMetrics(sourceReaderContext.metricGroup());
        return new FlinkSourceReader(
                elementsQueue, flussConfig, sourceReaderContext, flinkSourceReaderMetrics);
    }

    public static Builder newBuilder(Configuration flussConfig) {
        return new Builder(flussConfig);
    }

    /** Builder for {@link FlussDatabaseSyncSource}. */
    public static class Builder {
        private final Configuration flussConfig;

        private Filter<String> databaseFilter = Filter.alwaysTrue();
        private Filter<TableInfo> tableFilter = Filter.alwaysTrue();
        private NewTablesAddedListener newTablesAddedListener = tables -> {};

        private Builder(Configuration flussConfig) {
            this.flussConfig = flussConfig;
        }

        public Builder withTableFilter(Filter<TableInfo> tableFilter) {
            this.tableFilter = tableFilter;
            return this;
        }

        public Builder withDatabaseFilter(Filter<String> databaseFilter) {
            this.databaseFilter = databaseFilter;
            return this;
        }

        public Builder withNewTableAddedListener(NewTablesAddedListener newTablesAddedListener) {
            this.newTablesAddedListener = newTablesAddedListener;
            return this;
        }

        public FlussDatabaseSyncSource build() {
            return new FlussDatabaseSyncSource(
                    flussConfig, databaseFilter, tableFilter, newTablesAddedListener);
        }
    }
}
