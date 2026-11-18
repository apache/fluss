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
import org.apache.fluss.utils.json.JsonSerdeUtils;

import javax.annotation.Nullable;

/** One fixed physical target in a BulkLoad transaction. */
@Internal
public final class BulkLoadTarget {
    public final @Nullable Long partitionId;
    public final @Nullable String partitionName;
    public final int bucketCount;
    public final String remoteDataDir;
    public final boolean withdrawn;

    /** Creates a target record using the partition identity carried by its path. */
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

    /** Returns the same physical target withdrawn from the remaining task set. */
    public BulkLoadTarget withdraw() {
        return new BulkLoadTarget(partitionId, partitionName, bucketCount, remoteDataDir, true);
    }

    /** Encodes the version 1 JSON record. */
    public byte[] toJson() {
        return JsonSerdeUtils.writeValueAsBytes(
                this,
                (value, out) -> {
                    out.writeStartObject();
                    out.writeNumberField("version", 1);
                    if (partitionName != null) {
                        out.writeStringField("partition_name", partitionName);
                    }
                    out.writeNumberField("bucket_count", bucketCount);
                    out.writeStringField("remote_data_dir", remoteDataDir);
                    out.writeBooleanField("withdrawn", withdrawn);
                    out.writeEndObject();
                });
    }

    /** Decodes a record using the partition ID from its path. */
    public static BulkLoadTarget fromJson(@Nullable Long partitionId, byte[] bytes) {
        return JsonSerdeUtils.readValue(
                bytes,
                node ->
                        new BulkLoadTarget(
                                partitionId,
                                node.has("partition_name")
                                        ? node.get("partition_name").asText()
                                        : null,
                                node.get("bucket_count").asInt(),
                                node.get("remote_data_dir").asText(),
                                node.get("withdrawn").asBoolean()));
    }
}
