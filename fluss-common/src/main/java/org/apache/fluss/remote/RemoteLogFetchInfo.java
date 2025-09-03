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

package org.apache.fluss.remote;

import javax.annotation.Nullable;

import java.util.List;

/** Remote log fetch info. */
public class RemoteLogFetchInfo {
    private final String remoteLogTabletDir;
    private final @Nullable String partitionName;
    private final List<RemoteLogSegment> remoteLogSegmentList;
    private final int firstStartPos;

    public RemoteLogFetchInfo(
            String remoteLogTabletDir,
            @Nullable String partitionName,
            List<RemoteLogSegment> remoteLogSegmentList,
            int firstStartPos) {
        this.remoteLogTabletDir = remoteLogTabletDir;
        this.partitionName = partitionName;
        this.remoteLogSegmentList = remoteLogSegmentList;
        this.firstStartPos = firstStartPos;
    }

    public String remoteLogTabletDir() {
        return remoteLogTabletDir;
    }

    @Nullable
    public String partitionName() {
        return partitionName;
    }

    public List<RemoteLogSegment> remoteLogSegmentList() {
        return remoteLogSegmentList;
    }

    public int firstStartPos() {
        return firstStartPos;
    }

    @Override
    public String toString() {
        return "RemoteLogFetchInfo{"
                + "remoteLogTabletDir='"
                + remoteLogTabletDir
                + ", partitionName='"
                + partitionName
                + ", remoteLogSegmentList="
                + remoteLogSegmentList
                + ", firstStartPos="
                + firstStartPos
                + '}';
    }
}
