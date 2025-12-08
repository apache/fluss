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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import java.util.Map;

/** An event for notify lake table offset to local tablet servers. */
public class NotifyLakeTableOffsetEvent implements CoordinatorEvent {

    private final Map<Long, LakeTableSnapshot> lakeTableSnapshots;
    private final Map<TableBucket, Long> tableBucketMaxTieredTimestamps;

    public NotifyLakeTableOffsetEvent(
            Map<Long, LakeTableSnapshot> lakeTableSnapshots,
            Map<TableBucket, Long> tableBucketMaxTieredTimestamps) {
        this.lakeTableSnapshots = lakeTableSnapshots;
        this.tableBucketMaxTieredTimestamps = tableBucketMaxTieredTimestamps;
    }

    public Map<Long, LakeTableSnapshot> getLakeTableSnapshots() {
        return lakeTableSnapshots;
    }

    public Map<TableBucket, Long> getTableBucketMaxTieredTimestamps() {
        return tableBucketMaxTieredTimestamps;
    }
}
