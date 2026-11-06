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

package org.apache.fluss.server.index;

import org.apache.fluss.server.index.IndexReplicator.IndexProgressState;
import org.apache.fluss.server.index.IndexReplicator.ReadGroup;

import javax.annotation.Nullable;

import java.util.List;

/** Schedules reader work without owning replication progress or payload resources. */
interface IndexReplicationScheduler {

    void register(int workerId, IndexReplicator source);

    void unregister(int workerId, IndexReplicator expectedSource);

    /** Performs a bounded scan, stopping after the first actual read or construction attempt. */
    boolean pollWorker(int workerId);

    /** Called with the source lifecycle lock held. */
    @Nullable
    ReadGroup selectRead(IndexReplicator source, long highWatermark);

    /**
     * Returns null without transferring the pending result, or a group to construct immediately.
     * Called with the source lifecycle lock held.
     */
    @Nullable
    ReadGroup selectBuild(
            IndexReplicator source, ReadGroup pendingGroup, IndexSourceReader.ReadResult result);

    /** Releases input admission after the owned read has closed, retaining capacity retries. */
    void onReadClosed(IndexReplicator source);

    /** Selects optional ASYNC work only after the original SYNC group has been published. */
    List<IndexProgressState> selectAsync(
            IndexReplicator source, ReadGroup syncGroup, IndexSourceReader.ReadResult result);

    void onCapacityBlocked(IndexReplicator source, IndexProgressState index);

    void onWindowPublished(IndexReplicator source, IndexProgressState index);

    void onProgress(IndexReplicator source, IndexProgressState index);

    /** A null index denotes a shared source-read or decoding failure. */
    void onFailure(IndexReplicator source, @Nullable IndexProgressState index);
}
