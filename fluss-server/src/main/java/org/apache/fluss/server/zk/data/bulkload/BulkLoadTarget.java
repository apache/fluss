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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** One fixed physical target in a BulkLoad transaction. */
@Internal
public final class BulkLoadTarget {
    public final @Nullable Long partitionId;
    public final @Nullable String partitionName;
    public final int bucketCount;
    public final String remoteDataDir;
    public final boolean withdrawn;

    private BulkLoadTarget(
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

    /** Decodes a record using the partition ID from its path. */
    public static BulkLoadTarget fromJson(@Nullable Long partitionId, byte[] bytes) {
        return JsonSerdeUtils.readValue(
                bytes,
                node -> {
                    JsonNode version = node.path("version");
                    checkArgument(
                            version.isInt() && version.intValue() == 1,
                            "Unsupported BulkLoad target record version: %s",
                            version);
                    JsonNode partitionName = node.path("partition_name");
                    checkArgument(
                            partitionId == null
                                    ? partitionName.isMissingNode()
                                    : partitionName.isTextual(),
                            "BulkLoad target partition ID and name must be specified together.");
                    JsonNode bucketCount = node.path("bucket_count");
                    JsonNode remoteDataDir = node.path("remote_data_dir");
                    JsonNode withdrawn = node.path("withdrawn");
                    checkArgument(bucketCount.isInt(), "Invalid BulkLoad target bucket count.");
                    checkArgument(remoteDataDir.isTextual(), "Invalid BulkLoad target directory.");
                    checkArgument(
                            withdrawn.isBoolean(), "Invalid BulkLoad target withdrawal state.");
                    return new BulkLoadTarget(
                            partitionId,
                            partitionName.textValue(),
                            bucketCount.intValue(),
                            remoteDataDir.textValue(),
                            withdrawn.booleanValue());
                });
    }
}
