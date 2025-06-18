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

import com.alibaba.fluss.metadata.TableBucket;

import java.util.List;
import java.util.Optional;

/**
 * Class which stores {@link CompletedSnapshotHandle} to distributed coordination system(e.g.
 * Zookeeper, Kubernetes, etc.).
 */
public interface CompletedSnapshotHandleStore {

    /**
     * Persist the snapshot handle to coordination system(e.g. Zookeeper, Kubernetes, etc.).
     *
     * @param tableBucket the table bucket the snapshot belongs to
     * @param snapshotId the snapshot id
     * @param completedSnapshotHandle CompletedSnapshotHandle to be added
     * @throws Exception if persisting snapshot or writing snapshot handle failed
     */
    void add(
            TableBucket tableBucket,
            long snapshotId,
            CompletedSnapshotHandle completedSnapshotHandle)
            throws Exception;

    /**
     * Remove the snapshot handle for the given snapshot id of the given table bucket.
     *
     * @param tableBucket the table bucket the snapshot belongs to
     * @param snapshotId the snapshot id
     * @throws Exception if removing the handle failed
     */
    void remove(TableBucket tableBucket, long snapshotId) throws Exception;

    /**
     * Get the snapshot handle for the given snapshot id of the given table bucket.
     *
     * @param tableBucket the table bucket the snapshot belongs to
     * @param snapshotId the snapshot id
     * @return the snapshot handle
     * @throws Exception if getting the handle failed
     */
    Optional<CompletedSnapshotHandle> get(TableBucket tableBucket, long snapshotId)
            throws Exception;

    /**
     * Get all the completed snapshot handles order by snapshot id from the smallest to largest for
     * the given table bucket.
     *
     * @param tableBucket the table bucket the snapshot belongs to
     * @return the all completed snapshot order by snapshot id
     * @throws Exception if getting failed
     */
    List<CompletedSnapshotHandle> getAllCompletedSnapshotHandles(TableBucket tableBucket)
            throws Exception;

    /**
     * Get the latest completed snapshot handle for the given table bucket.
     *
     * @param tableBucket the table bucket the snapshot belongs to
     * @return the latest completed snapshot handle, empty if no any snapshot
     * @throws Exception if getting failed
     */
    Optional<CompletedSnapshotHandle> getLatestCompletedSnapshotHandle(TableBucket tableBucket)
            throws Exception;
}
