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
import org.apache.fluss.rpc.entity.ResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;

/** The offsets of a bucket after its leader has frozen writes for partition retention. */
public class FreezePartitionResultForBucket extends ResultForBucket {

    private final long highWatermark;
    private final long logEndOffset;

    public FreezePartitionResultForBucket(
            TableBucket tableBucket, long highWatermark, long logEndOffset) {
        super(tableBucket);
        this.highWatermark = highWatermark;
        this.logEndOffset = logEndOffset;
    }

    public FreezePartitionResultForBucket(TableBucket tableBucket, ApiError error) {
        super(tableBucket, error);
        this.highWatermark = -1L;
        this.logEndOffset = -1L;
    }

    public long getHighWatermark() {
        return highWatermark;
    }

    public long getLogEndOffset() {
        return logEndOffset;
    }
}
