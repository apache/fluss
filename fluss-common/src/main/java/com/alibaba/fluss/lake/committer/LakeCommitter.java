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

package com.alibaba.fluss.lake.committer;

import com.alibaba.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The LakeCommitter interface for committing write results. It extends the AutoCloseable interface
 * to ensure resources are released after use.
 *
 * @param <WriteResult> the type of the write result
 * @param <CommittableT> the type of the committable object
 * @since 0.7
 */
@PublicEvolving
public interface LakeCommitter<WriteResult, CommittableT> extends AutoCloseable {

    /**
     * Converts a list of write results to a committable object.
     *
     * @param writeResults the list of write results
     * @return the committable object
     * @throws IOException if an I/O error occurs
     */
    CommittableT toCommittable(List<WriteResult> writeResults) throws IOException;

    /**
     * Commits the given committable object.
     *
     * @param committable the committable object
     * @param snapshotProperties the properties that lake supported to store in snapshot
     * @return the committed snapshot ID
     * @throws IOException if an I/O error occurs
     */
    long commit(CommittableT committable, Map<String, String> snapshotProperties)
            throws IOException;

    /**
     * Aborts the given committable object.
     *
     * @param committable the committable object
     * @throws IOException if an I/O error occurs
     */
    void abort(CommittableT committable) throws IOException;

    /**
     * Get missing lake snapshot that has been committed to lake but didn't commit to fluss.
     *
     * @param latestLakeSnapshotIdOfFluss the latest lake snapshot id in fluss, used to judge which
     *     lake snapshot is missing.
     * @return the missing lake snapshot, returns null if no any missing snapshot found
     * @throws IOException if an I/O error occurs
     */
    @Nullable
    CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException;
}
