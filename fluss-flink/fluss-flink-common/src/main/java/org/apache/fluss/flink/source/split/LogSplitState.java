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

package org.apache.fluss.flink.source.split;

import static org.apache.fluss.flink.source.split.LogSplit.NO_STOPPING_OFFSET;

/** The state of {@link LogSplit}. */
public class LogSplitState extends SourceSplitState {

    /** The next log offset to read. */
    private long nextOffset;

    public LogSplitState(LogSplit split) {
        super(split);
        this.nextOffset = split.getStartingOffset();
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    @Override
    public LogSplit toSourceSplit() {
        final LogSplit logSplit = (LogSplit) split;
        return new LogSplit(
                logSplit.tableBucket,
                logSplit.getPartitionName(),
                nextOffset,
                logSplit.getStoppingOffset().orElse(NO_STOPPING_OFFSET));
    }
}
