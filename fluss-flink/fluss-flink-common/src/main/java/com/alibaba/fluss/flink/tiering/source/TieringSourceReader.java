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

package com.alibaba.fluss.flink.tiering.source;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.tiering.source.split.TieringSplit;
import com.alibaba.fluss.flink.tiering.source.state.TieringSplitState;
import com.alibaba.fluss.lake.writer.LakeTieringFactory;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** A {@link SourceReader} that read records from Fluss and write to lake. */
@Internal
public final class TieringSourceReader<WriteResult>
        extends SingleThreadMultiplexSourceReaderBase<
                TableBucketWriteResult<WriteResult>,
                TableBucketWriteResult<WriteResult>,
                TieringSplit,
                TieringSplitState> {

    public TieringSourceReader(
            SourceReaderContext context,
            Configuration flussConf,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory) {
        super(
                () -> new TieringSplitReader<>(flussConf, lakeTieringFactory),
                new TableBucketWriteResultEmitter<>(),
                context.getConfiguration(),
                context);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, TieringSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    public List<TieringSplit> snapshotState(long checkpointId) {
        // we return empty list to make source reader be stateless
        return Collections.emptyList();
    }

    @Override
    protected TieringSplitState initializedState(TieringSplit split) {
        if (split.isTieringSnapshotSplit()) {
            return new TieringSplitState(split);
        } else if (split.isTieringLogSplit()) {
            return new TieringSplitState(split);
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }
    }

    @Override
    protected TieringSplit toSplitType(String splitId, TieringSplitState splitState) {
        return splitState.toSourceSplit();
    }
}
