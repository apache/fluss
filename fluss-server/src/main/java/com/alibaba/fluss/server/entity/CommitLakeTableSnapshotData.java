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

import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import com.alibaba.fluss.server.zk.data.LakeTableSnapshot;

import java.util.Map;
import java.util.Objects;

/** The data for request {@link CommitLakeTableSnapshotRequest}. */
public class CommitLakeTableSnapshotData {

    private final Map<Long, LakeTableSnapshot> lakeTableSnapshots;

    public CommitLakeTableSnapshotData(Map<Long, LakeTableSnapshot> lakeTableSnapshots) {
        this.lakeTableSnapshots = lakeTableSnapshots;
    }

    public Map<Long, LakeTableSnapshot> getLakeTableSnapshot() {
        return lakeTableSnapshots;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CommitLakeTableSnapshotData)) {
            return false;
        }
        CommitLakeTableSnapshotData that = (CommitLakeTableSnapshotData) o;
        return Objects.equals(lakeTableSnapshots, that.lakeTableSnapshots);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(lakeTableSnapshots);
    }

    @Override
    public String toString() {
        return "CommitLakeTableSnapshotData{" + "lakeTableInfos=" + lakeTableSnapshots + '}';
    }
}
