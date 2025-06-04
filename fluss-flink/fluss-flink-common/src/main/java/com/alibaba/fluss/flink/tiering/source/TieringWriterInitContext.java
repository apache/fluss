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

import com.alibaba.fluss.lakehouse.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

/** The implementation of {@link WriterInitContext}. */
public class TieringWriterInitContext implements WriterInitContext {

    private final TablePath tablePath;
    private final TableBucket tableBucket;
    @Nullable private final String partition;

    public TieringWriterInitContext(TablePath tablePath, TableBucket tableBucket) {
        this(tablePath, tableBucket, null);
    }

    public TieringWriterInitContext(
            TablePath tablePath, TableBucket tableBucket, @Nullable String partition) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.partition = partition;
    }

    @Override
    public TablePath tablePath() {
        return tablePath;
    }

    @Override
    public TableBucket tableBucket() {
        return tableBucket;
    }

    @Nullable
    @Override
    public String partition() {
        return partition;
    }
}
