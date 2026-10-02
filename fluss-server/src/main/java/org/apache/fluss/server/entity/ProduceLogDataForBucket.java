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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.MemoryLogRecords;

import javax.annotation.Nullable;

/** Log records for one normal or historical partition bucket. */
public class ProduceLogDataForBucket {
    private final TableBucket tableBucket;
    private final MemoryLogRecords records;
    // Identifies the original partition for a historical write; null for a normal write.
    private final @Nullable String originalPartitionName;

    public ProduceLogDataForBucket(
            TableBucket tableBucket,
            MemoryLogRecords records,
            @Nullable String originalPartitionName) {
        this.tableBucket = tableBucket;
        this.records = records;
        this.originalPartitionName = originalPartitionName;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    public MemoryLogRecords records() {
        return records;
    }

    public @Nullable String originalPartitionName() {
        return originalPartitionName;
    }
}
