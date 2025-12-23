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
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

/** The data for request {@link CommitLakeTableSnapshotRequest}. */
public class CommitLakeTableSnapshotData {

    private final Map<Long, LakeTableSnapshot> lakeTableSnapshots;
    private final Map<TableBucket, Long> tableBucketsMaxTieredTimestamp;

    // the serialization version for lake table snapshot, will be null
    // before 0.8
    private final Integer serializationVersion;

    public CommitLakeTableSnapshotData(
            Map<Long, LakeTableSnapshot> lakeTableSnapshots,
            Map<TableBucket, Long> tableBucketsMaxTieredTimestamp,
            @Nullable Integer serializationVersion) {
        this.lakeTableSnapshots = lakeTableSnapshots;
        this.tableBucketsMaxTieredTimestamp = tableBucketsMaxTieredTimestamp;
        this.serializationVersion = serializationVersion;
    }

    public Map<Long, LakeTableSnapshot> getLakeTableSnapshot() {
        return lakeTableSnapshots;
    }

    public Map<TableBucket, Long> getTableBucketsMaxTieredTimestamp() {
        return tableBucketsMaxTieredTimestamp;
    }

    public Integer getSerializationVersion() {
        return serializationVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommitLakeTableSnapshotData that = (CommitLakeTableSnapshotData) o;
        return Objects.equals(lakeTableSnapshots, that.lakeTableSnapshots)
                && Objects.equals(
                        tableBucketsMaxTieredTimestamp, that.tableBucketsMaxTieredTimestamp)
                && Objects.equals(serializationVersion, that.serializationVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                lakeTableSnapshots, tableBucketsMaxTieredTimestamp, serializationVersion);
    }

    @Override
    public String toString() {
        return "CommitLakeTableSnapshotData{"
                + "lakeTableSnapshots="
                + lakeTableSnapshots
                + ", tableBucketsMaxTieredTimestamp="
                + tableBucketsMaxTieredTimestamp
                + ", serializationVersion="
                + serializationVersion
                + '}';
    }
}
