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

package org.apache.fluss.lake.committer;

import java.util.Map;
import java.util.Objects;

/**
 * The lake already committed snapshot, containing the lake snapshot id and the properties stored in
 * this snapshot.
 */
public class CommittedLakeSnapshot {

    private final long lakeSnapshotId;

    private final Map<String, String> snapshotProperties;

    public CommittedLakeSnapshot(long lakeSnapshotId, Map<String, String> snapshotProperties) {
        this.lakeSnapshotId = lakeSnapshotId;
        this.snapshotProperties = snapshotProperties;
    }

    public long getLakeSnapshotId() {
        return lakeSnapshotId;
    }

    public Map<String, String> getSnapshotProperties() {
        return snapshotProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommittedLakeSnapshot that = (CommittedLakeSnapshot) o;
        return lakeSnapshotId == that.lakeSnapshotId
                && Objects.equals(snapshotProperties, that.snapshotProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lakeSnapshotId, snapshotProperties);
    }

    @Override
    public String toString() {
        return "CommittedLakeSnapshot{"
                + "lakeSnapshotId="
                + lakeSnapshotId
                + ", snapshotProperties="
                + snapshotProperties
                + '}';
    }
}
