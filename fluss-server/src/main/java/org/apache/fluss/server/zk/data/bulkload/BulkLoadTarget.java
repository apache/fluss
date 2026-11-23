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

    private BulkLoadTarget(@Nullable Long partitionId, JsonNode record) {
        JsonNode version = record.path("version");
        checkArgument(
                version.isInt() && version.intValue() == 1, "Unsupported BulkLoad target version.");
        this.partitionId = partitionId;
        this.partitionName = record.path("partition_name").textValue();
        checkArgument(
                partitionId == null ? !record.has("partition_name") : partitionName != null,
                "BulkLoad target partition ID and name must be specified together.");
        JsonNode count = record.path("bucket_count");
        checkArgument(count.isInt(), "Invalid BulkLoad target bucket count.");
        this.bucketCount = count.intValue();
        this.remoteDataDir = record.path("remote_data_dir").textValue();
        checkArgument(remoteDataDir != null, "Invalid BulkLoad target directory.");
        JsonNode withdrawn = record.path("withdrawn");
        checkArgument(withdrawn.isBoolean(), "Invalid BulkLoad target withdrawal state.");
        this.withdrawn = withdrawn.booleanValue();
    }

    /** Decodes a record using the partition ID from its path. */
    public static BulkLoadTarget fromJson(@Nullable Long partitionId, byte[] bytes) {
        return JsonSerdeUtils.readValue(bytes, node -> new BulkLoadTarget(partitionId, node));
    }
}
