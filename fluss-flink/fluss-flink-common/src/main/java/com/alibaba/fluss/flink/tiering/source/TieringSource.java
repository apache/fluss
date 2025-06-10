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

package com.alibaba.fluss.flink.tiering.source;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator;
import com.alibaba.fluss.flink.tiering.source.split.TieringSplit;
import com.alibaba.fluss.flink.tiering.source.split.TieringSplitSerializer;
import com.alibaba.fluss.flink.tiering.source.state.TieringSourceEnumeratorState;
import com.alibaba.fluss.flink.tiering.source.state.TieringSourceEnumeratorStateSerializer;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import static com.alibaba.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;

/**
 * The flink source implementation for tiering data from Fluss to downstream lake.
 *
 * @param <WriteResult> the type of write lake result.
 */
public class TieringSource<WriteResult>
        implements Source<
                TableBucketWriteResult<WriteResult>, TieringSplit, TieringSourceEnumeratorState> {

    private final Configuration flussConf;
    private final LakeTieringFactory<WriteResult, ?> lakeTieringFactory;
    private final long pollTieringTableIntervalMs;

    public TieringSource(
            Configuration flussConf,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory,
            long pollTieringTableIntervalMs) {
        this.flussConf = flussConf;
        this.lakeTieringFactory = lakeTieringFactory;
        this.pollTieringTableIntervalMs = pollTieringTableIntervalMs;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<TieringSplit, TieringSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<TieringSplit> splitEnumeratorContext) throws Exception {
        return new TieringSourceEnumerator(
                flussConf, splitEnumeratorContext, pollTieringTableIntervalMs);
    }

    @Override
    public SplitEnumerator<TieringSplit, TieringSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<TieringSplit> splitEnumeratorContext,
            TieringSourceEnumeratorState tieringSourceEnumeratorState)
            throws Exception {
        // stateless operator
        return new TieringSourceEnumerator(
                flussConf, splitEnumeratorContext, pollTieringTableIntervalMs);
    }

    @Override
    public SimpleVersionedSerializer<TieringSplit> getSplitSerializer() {
        return TieringSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<TieringSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return TieringSourceEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public SourceReader<TableBucketWriteResult<WriteResult>, TieringSplit> createReader(
            SourceReaderContext sourceReaderContext) throws Exception {
        return new TieringSourceReader<>(sourceReaderContext, flussConf, lakeTieringFactory);
    }

    /** Builder for {@link TieringSource}. */
    public static class Builder<WriteResult> {

        private final Configuration flussConf;
        private final LakeTieringFactory<WriteResult, ?> lakeTieringFactory;
        private long pollTieringTableIntervalMs =
                POLL_TIERING_TABLE_INTERVAL.defaultValue().toMillis();

        public Builder(
                Configuration flussConf, LakeTieringFactory<WriteResult, ?> lakeTieringFactory) {
            this.flussConf = flussConf;
            this.lakeTieringFactory = lakeTieringFactory;
        }

        public Builder<WriteResult> withPollTieringTableIntervalMs(long pollTieringTableInterval) {
            this.pollTieringTableIntervalMs = pollTieringTableInterval;
            return this;
        }

        public TieringSource<WriteResult> build() {
            return new TieringSource<>(flussConf, lakeTieringFactory, pollTieringTableIntervalMs);
        }
    }
}
