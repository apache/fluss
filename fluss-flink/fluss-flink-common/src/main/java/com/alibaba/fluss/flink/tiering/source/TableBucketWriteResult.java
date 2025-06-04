/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.source;

import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * This class contains the {@link WriteResult} of {@link LakeWriter}, the table path and the bucket
 * that the write result is for, the end log offset of tiering. It'll be passed to downstream
 * operators to collect all the write results of a table and do commit.
 */
public class TableBucketWriteResult<WriteResult> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;

    private final TableBucket tableBucket;

    @Nullable private final WriteResult writeResult;

    // the end offset of tiering, should be the last tiered record's offset + 1
    private final long logEndOffset;

    public TableBucketWriteResult(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable WriteResult writeResult,
            long logEndOffset) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.writeResult = writeResult;
        this.logEndOffset = logEndOffset;
    }

    public TablePath tablePath() {
        return tablePath;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    @Nullable
    public WriteResult writeResult() {
        return writeResult;
    }

    public long logEndOffset() {
        return logEndOffset;
    }
}
