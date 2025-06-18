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

package com.alibaba.fluss.utils.json;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/** Json serializer and deserializer for {@link TablePartition}. */
@Internal
public class TablePartitionJsonSerde
        implements JsonSerializer<TablePartition>, JsonDeserializer<TablePartition> {

    public static final TablePartitionJsonSerde INSTANCE = new TablePartitionJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String TABLE_ID_KEY = "table_id";
    private static final String PARTITION_ID_KEY = "partition_id";
    private static final int VERSION = 1;

    @Override
    public void serialize(TablePartition partition, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(TABLE_ID_KEY, partition.getTableId());
        generator.writeNumberField(PARTITION_ID_KEY, partition.getPartitionId());
        generator.writeEndObject();
    }

    @Override
    public TablePartition deserialize(JsonNode node) {
        long tableId = node.get(TABLE_ID_KEY).asLong();
        long partitionId = node.get(PARTITION_ID_KEY).asLong();
        return new TablePartition(tableId, partitionId);
    }
}
