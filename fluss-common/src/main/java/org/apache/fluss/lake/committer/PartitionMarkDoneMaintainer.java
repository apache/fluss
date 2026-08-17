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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.utils.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * An optional capability that a {@link LakeCommitter} may implement to persist the partition
 * mark-done state for a tiering round without any data to commit.
 */
@Internal
public interface PartitionMarkDoneMaintainer {

    /**
     * Performs partition mark-done maintenance for a tiering round without any data to commit.
     *
     * @param offsetsFileProvider provides a freshly prepared bucket offsets file for the
     *     maintenance snapshot; it is only invoked when a snapshot will actually be created. Every
     *     snapshot must carry its own offsets file since offsets files are deleted along with their
     *     snapshot metadata and thus must not be shared across snapshots.
     * @return the properties-only lake snapshot created to persist the mark-done state, or null if
     *     no snapshot was created (feature disabled or state unchanged)
     * @throws IOException if an I/O error occurs
     */
    @Nullable
    CommittedLakeSnapshot commitMarkDoneMaintenance(
            SupplierWithException<String, IOException> offsetsFileProvider) throws IOException;
}
