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

import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * This class contains the {@link WriteResult} of {@link LakeWriter}, the table path and the bucket
 * that the write result is for, the end log offset of tiering, the total number of write results in
 * one round of tiering. It'll be passed to downstream committer operator to collect all the write
 * results of a table and do commit.
 */
public class TableBucketWriteResult<WriteResult> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;

    private final TableBucket tableBucket;

    // null when the bucket is not for a partition
    @Nullable private final String partitionName;

    // will be null when no any data write, such as for tiering a empty log split
    @Nullable private final WriteResult writeResult;

    // the end offset of tiering, should be the last tiered record's offset + 1
    private final long logEndOffset;

    // the total number of write results in one round of tiering,
    // used for downstream commiter operator to determine when all write results
    // for the round of tiering is finished
    private final int numberOfWriteResults;

    public TableBucketWriteResult(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable WriteResult writeResult,
            long logEndOffset,
            int numberOfWriteResults) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        this.writeResult = writeResult;
        this.logEndOffset = logEndOffset;
        this.numberOfWriteResults = numberOfWriteResults;
    }

    public TablePath tablePath() {
        return tablePath;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    @Nullable
    public String partitionName() {
        return partitionName;
    }

    @Nullable
    public WriteResult writeResult() {
        return writeResult;
    }

    public int numberOfWriteResults() {
        return numberOfWriteResults;
    }

    public long logEndOffset() {
        return logEndOffset;
    }
}
