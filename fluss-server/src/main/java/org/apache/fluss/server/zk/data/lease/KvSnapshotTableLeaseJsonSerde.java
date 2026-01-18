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

package org.apache.fluss.server.zk.data.lease;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Json serializer and deserializer for {@link KvSnapshotTableLease}. */
public class KvSnapshotTableLeaseJsonSerde
        implements JsonSerializer<KvSnapshotTableLease>, JsonDeserializer<KvSnapshotTableLease> {

    public static final KvSnapshotTableLeaseJsonSerde INSTANCE =
            new KvSnapshotTableLeaseJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_SNAPSHOTS = "partition_snapshots";
    private static final String PARTITION_ID = "partition_id";
    private static final String BUCKET_SNAPSHOTS = "bucket_snapshots";

    private static final int VERSION = 1;

    @Override
    public void serialize(KvSnapshotTableLease kvSnapshotTableLease, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(TABLE_ID, kvSnapshotTableLease.getTableId());

        if (kvSnapshotTableLease.getBucketSnapshots() != null) {
            // for none-partition table.
            generator.writeArrayFieldStart(BUCKET_SNAPSHOTS);
            for (Long snapshot : kvSnapshotTableLease.getBucketSnapshots()) {
                generator.writeNumber(snapshot);
            }
            generator.writeEndArray();
        } else {
            // for partition table.
            Map<Long, Long[]> partitionSnapshots = kvSnapshotTableLease.getPartitionSnapshots();
            if (partitionSnapshots != null && !partitionSnapshots.isEmpty()) {
                generator.writeArrayFieldStart(PARTITION_SNAPSHOTS);
                for (Map.Entry<Long, Long[]> entry : partitionSnapshots.entrySet()) {
                    generator.writeStartObject();
                    generator.writeNumberField(PARTITION_ID, entry.getKey());
                    generator.writeArrayFieldStart(BUCKET_SNAPSHOTS);
                    for (Long snapshot : entry.getValue()) {
                        generator.writeNumber(snapshot);
                    }
                    generator.writeEndArray();
                    generator.writeEndObject();
                }
                generator.writeEndArray();
            }
        }
    }

    @Override
    public KvSnapshotTableLease deserialize(JsonNode node) {
        long tableId = node.get(TABLE_ID).asLong();
        if (node.has(BUCKET_SNAPSHOTS)) {
            // for none-partition table.
            Long[] bucketSnapshots = new Long[node.get(BUCKET_SNAPSHOTS).size()];
            for (int i = 0; i < bucketSnapshots.length; i++) {
                bucketSnapshots[i] = node.get(BUCKET_SNAPSHOTS).get(i).asLong();
            }
            return new KvSnapshotTableLease(tableId, bucketSnapshots);
        } else {
            // for partition table.
            Map<Long, Long[]> partitionSnapshots = new HashMap<>();
            JsonNode partitionSnapshotsNode = node.get(PARTITION_SNAPSHOTS);
            for (JsonNode partitionSnapshotNode : partitionSnapshotsNode) {
                long partitionId = partitionSnapshotNode.get(PARTITION_ID).asLong();
                Long[] bucketSnapshots =
                        new Long[partitionSnapshotNode.get(BUCKET_SNAPSHOTS).size()];
                for (int i = 0; i < bucketSnapshots.length; i++) {
                    bucketSnapshots[i] =
                            partitionSnapshotNode.get(BUCKET_SNAPSHOTS).get(i).asLong();
                }
                partitionSnapshots.put(partitionId, bucketSnapshots);
            }
            return new KvSnapshotTableLease(tableId, partitionSnapshots);
        }
    }

    /** Serialize the {@link KvSnapshotTableLease} to json bytes using current version. */
    public static byte[] toJson(KvSnapshotTableLease kvSnapshotTableLease) {
        return JsonSerdeUtils.writeValueAsBytes(kvSnapshotTableLease, INSTANCE);
    }

    /** Deserialize the json bytes to {@link KvSnapshotTableLease}. */
    public static KvSnapshotTableLease fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }
}
