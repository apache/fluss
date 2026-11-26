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

package org.apache.fluss.server.zk.data.bulkload;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

/** One fixed physical target in a BulkLoad transaction. */
@Internal
public final class BulkLoadTarget {
    public final @Nullable Long partitionId;
    public final @Nullable String partitionName;
    public final int bucketCount;
    public final String remoteDataDir;
    public final boolean withdrawn;

    /** Creates a target record. */
    public BulkLoadTarget(
            @Nullable Long partitionId,
            @Nullable String partitionName,
            int bucketCount,
            String remoteDataDir,
            boolean withdrawn) {
        this.partitionId = partitionId;
        this.partitionName = partitionName;
        this.bucketCount = bucketCount;
        this.remoteDataDir = remoteDataDir;
        this.withdrawn = withdrawn;
    }

    /** Returns the representation of this target after its partition is dropped. */
    public BulkLoadTarget withdraw() {
        return new BulkLoadTarget(partitionId, partitionName, bucketCount, remoteDataDir, true);
    }
}
