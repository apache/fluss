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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** The implementation of {@link WriterInitContext}. */
public class TieringWriterInitContext implements WriterInitContext {

    private final TablePath tablePath;
    private final TableBucket tableBucket;
    @Nullable private final String partition;
    private final TableInfo tableInfo;
    private final int splitIndex;
    private final long tieringRoundTimestamp;
    private final int bucketCount;
    @Nullable private final String[] ioTmpDirs;

    public TieringWriterInitContext(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partition,
            TableInfo tableInfo) {
        this(
                tablePath,
                tableBucket,
                partition,
                tableInfo,
                UNKNOWN_SPLIT_INDEX,
                UNKNOWN_TIERING_ROUND_TIMESTAMP,
                null,
                null);
    }

    public TieringWriterInitContext(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partition,
            TableInfo tableInfo,
            int splitIndex,
            long tieringRoundTimestamp) {
        this(
                tablePath,
                tableBucket,
                partition,
                tableInfo,
                splitIndex,
                tieringRoundTimestamp,
                null,
                null);
    }

    public TieringWriterInitContext(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partition,
            TableInfo tableInfo,
            int splitIndex,
            long tieringRoundTimestamp,
            @Nullable Integer partitionBucketCount,
            @Nullable String[] ioTmpDirs) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.partition = partition;
        this.tableInfo = tableInfo;
        this.splitIndex = splitIndex;
        this.tieringRoundTimestamp = tieringRoundTimestamp;
        this.ioTmpDirs = ioTmpDirs;
        if (tableBucket.getPartitionId() == null) {
            this.bucketCount = tableInfo.getNumBuckets();
        } else {
            // Writing with a wrong bucket count would silently corrupt the lake table's bucket
            // layout metadata, so a missing per-partition count must fail here.
            this.bucketCount =
                    checkNotNull(
                            partitionBucketCount,
                            "No actual bucket count known for partition %s (id %s) of table %s.",
                            partition,
                            tableBucket.getPartitionId(),
                            tablePath);
        }
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

    @Override
    public TableInfo tableInfo() {
        return tableInfo;
    }

    @Override
    public int splitIndex() {
        return splitIndex;
    }

    @Override
    public long tieringRoundTimestamp() {
        return tieringRoundTimestamp;
    }

    @Nullable
    @Override
    public String[] ioTmpDirs() {
        return ioTmpDirs;
    }

    @Override
    public int bucketCount() {
        return bucketCount;
    }
}
