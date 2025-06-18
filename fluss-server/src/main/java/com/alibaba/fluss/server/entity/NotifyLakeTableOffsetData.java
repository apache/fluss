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

package com.alibaba.fluss.server.entity;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetRequest;

import java.util.Map;

/** The data for request {@link NotifyLakeTableOffsetRequest}. */
public class NotifyLakeTableOffsetData {

    private final int coordinatorEpoch;

    private final Map<TableBucket, LakeBucketOffset> lakeBucketOffsets;

    public NotifyLakeTableOffsetData(
            int coordinatorEpoch, Map<TableBucket, LakeBucketOffset> lakeBucketOffsets) {
        this.coordinatorEpoch = coordinatorEpoch;
        this.lakeBucketOffsets = lakeBucketOffsets;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public Map<TableBucket, LakeBucketOffset> getLakeBucketOffsets() {
        return lakeBucketOffsets;
    }
}
