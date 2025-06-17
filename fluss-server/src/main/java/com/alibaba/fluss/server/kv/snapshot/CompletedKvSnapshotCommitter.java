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

package com.alibaba.fluss.server.kv.snapshot;

/** An interface for reporting a {@link CompletedSnapshot}. */
public interface CompletedKvSnapshotCommitter {

    /**
     * Commit the completed kv snapshot for a bucket.
     *
     * @param snapshot CompletedSnapshot to be reported
     * @param coordinatorEpoch the coordinator epoch when the snapshot is triggered
     * @param bucketLeaderEpoch the bucket leader epoch when the snapshot is triggered
     * @throws Exception if report completed snapshot failed
     */
    void commitKvSnapshot(CompletedSnapshot snapshot, int coordinatorEpoch, int bucketLeaderEpoch)
            throws Exception;
}
