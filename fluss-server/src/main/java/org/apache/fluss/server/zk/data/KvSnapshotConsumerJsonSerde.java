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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Json serializer and deserializer for {@link KvSnapshotConsumer}. */
public class KvSnapshotConsumerJsonSerde
        implements JsonSerializer<KvSnapshotConsumer>, JsonDeserializer<KvSnapshotConsumer> {

    public static final KvSnapshotConsumerJsonSerde INSTANCE = new KvSnapshotConsumerJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String EXPIRATION_TIME = "expiration_time";
    private static final String TABLES = "tables";

    private static final int VERSION = 1;

    @Override
    public void serialize(KvSnapshotConsumer kvSnapshotConsumer, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(EXPIRATION_TIME, kvSnapshotConsumer.getExpirationTime());

        generator.writeFieldName(TABLES);
        generator.writeStartObject();

        // for none-partitioned table.
        for (Map.Entry<Long, Long[]> entry :
                kvSnapshotConsumer.getTableIdToSnapshots().entrySet()) {
            generator.writeFieldName(entry.getKey().toString());
            writeLongArray(generator, entry.getValue());
        }

        // for partitioned table.
        Map<Long, Long[]> partitionToSortedSnapshotIds =
                kvSnapshotConsumer.getPartitionIdToSnapshots();
        for (Map.Entry<Long, Set<Long>> entry :
                kvSnapshotConsumer.getTableIdToPartitions().entrySet()) {
            Long tableId = entry.getKey();

            generator.writeFieldName(tableId.toString());
            generator.writeStartObject();

            for (Long partitionId : entry.getValue()) {
                Long[] snapshotIds = partitionToSortedSnapshotIds.get(partitionId);
                if (snapshotIds != null) {
                    generator.writeFieldName(partitionId.toString());
                    writeLongArray(generator, snapshotIds);
                }
            }

            generator.writeEndObject();
        }
        generator.writeEndObject(); // tables

        generator.writeEndObject(); // root
    }

    @Override
    public KvSnapshotConsumer deserialize(JsonNode node) {
        long expirationTime = node.get(EXPIRATION_TIME).asLong();
        JsonNode tablesNode = node.get(TABLES);

        Map<Long, Long[]> tableIdToSnapshots = new HashMap<>();
        Map<Long, Set<Long>> tableIdToPartitions = new HashMap<>();
        Map<Long, Long[]> partitionIdToSnapshots = new HashMap<>();

        ObjectNode tablesObj = (ObjectNode) tablesNode;
        Iterator<Map.Entry<String, JsonNode>> tableFields = tablesObj.fields();
        while (tableFields.hasNext()) {
            Map.Entry<String, JsonNode> tableEntry = tableFields.next();
            Long tableId = Long.parseLong(tableEntry.getKey());
            JsonNode tableValue = tableEntry.getValue();

            if (tableValue.isArray()) {
                // Non-partitioned table, like: "1": [1, -1, 1, 2]
                List<Long> snapshotIds = new ArrayList<>();
                for (JsonNode elem : tableValue) {
                    snapshotIds.add(elem.asLong());
                }
                tableIdToSnapshots.put(tableId, snapshotIds.toArray(new Long[0]));
            } else if (tableValue.isObject()) {
                // Partitioned table, like: "2": { "1001": [...], "1002": [...] }
                Set<Long> partitions = new HashSet<>();
                ObjectNode partitionObj = (ObjectNode) tableValue;
                Iterator<Map.Entry<String, JsonNode>> partFields = partitionObj.fields();
                while (partFields.hasNext()) {
                    Map.Entry<String, JsonNode> partEntry = partFields.next();
                    Long partitionId = Long.parseLong(partEntry.getKey());
                    JsonNode partArray = partEntry.getValue();

                    if (!partArray.isArray()) {
                        throw new IllegalArgumentException(
                                "Partition value must be array for table "
                                        + tableId
                                        + ", partition "
                                        + partitionId);
                    }

                    List<Long> snapshotIds = new ArrayList<>();
                    for (JsonNode elem : partArray) {
                        snapshotIds.add(elem.asLong());
                    }

                    partitions.add(partitionId);
                    partitionIdToSnapshots.put(partitionId, snapshotIds.toArray(new Long[0]));
                }

                if (!partitions.isEmpty()) {
                    tableIdToPartitions.put(tableId, partitions);
                }
            } else {
                throw new IllegalArgumentException(
                        "Table value must be array or object: " + tableId);
            }
        }

        return new KvSnapshotConsumer(
                expirationTime, tableIdToSnapshots, tableIdToPartitions, partitionIdToSnapshots);
    }

    private void writeLongArray(JsonGenerator generator, Long[] sortedSnapshotIds)
            throws IOException {
        generator.writeStartArray();
        for (Long value : sortedSnapshotIds) {
            generator.writeNumber(value);
        }
        generator.writeEndArray();
    }
}
