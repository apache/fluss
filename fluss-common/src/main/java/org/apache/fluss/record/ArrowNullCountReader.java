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

package org.apache.fluss.record;

import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.FieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.Message;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.RecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.shaded.guava32.com.google.common.cache.Cache;
import org.apache.fluss.shaded.guava32.com.google.common.cache.CacheBuilder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Utility for extracting null counts from Arrow RecordBatch FlatBuffer metadata. Used by both
 * FileChannelLogRecordBatch and DefaultLogRecordBatch to read null counts from Arrow metadata
 * instead of the statistics binary format (V2).
 */
class ArrowNullCountReader {

    private static final Cache<FieldNodeMappingKey, int[]> FIELD_NODE_MAPPING_CACHE =
            CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterAccess(Duration.ofMinutes(10))
                    .build();

    /**
     * Cached version of {@link #computeFieldNodeMapping}. Uses a bounded Guava cache keyed by
     * (schemaId, statsIndexMapping). Safe for concurrent access. The cache is bounded to 1000
     * entries with 10-minute expiry to prevent unbounded growth during schema evolution.
     */
    static int[] computeFieldNodeMappingCached(
            RowType rowType, int[] statsIndexMapping, int schemaId) {
        FieldNodeMappingKey key = new FieldNodeMappingKey(schemaId, statsIndexMapping);
        int[] cached = FIELD_NODE_MAPPING_CACHE.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        int[] result = computeFieldNodeMapping(rowType, statsIndexMapping);
        FIELD_NODE_MAPPING_CACHE.put(key, result);
        return result;
    }

    /**
     * Compute mapping from statistics column positions to Arrow FieldNode indexes. Handles nested
     * types (ARRAY, MAP, ROW) which occupy multiple FieldNode slots.
     *
     * @param rowType the full table schema
     * @param statsIndexMapping maps statistics positions to schema field indexes
     * @return array where result[i] is the FieldNode index for statsIndexMapping[i]
     */
    static int[] computeFieldNodeMapping(RowType rowType, int[] statsIndexMapping) {
        Schema arrowSchema = ArrowUtils.toArrowSchema(rowType);
        List<Field> fields = arrowSchema.getFields();

        // Build schema-field-index to FieldNode-index mapping
        int[] schemaToFieldNode = new int[rowType.getFieldCount()];
        int fieldNodeIndex = 0;
        for (int i = 0; i < fields.size(); i++) {
            schemaToFieldNode[i] = fieldNodeIndex;
            fieldNodeIndex += countFieldNodes(fields.get(i));
        }

        // Map statsIndexMapping to FieldNode indexes
        int[] result = new int[statsIndexMapping.length];
        for (int i = 0; i < statsIndexMapping.length; i++) {
            result[i] = schemaToFieldNode[statsIndexMapping[i]];
        }
        return result;
    }

    /** Count total FieldNodes for a field (1 for simple types, more for nested). */
    private static int countFieldNodes(Field field) {
        int count = 1; // the field itself
        for (Field child : field.getChildren()) {
            count += countFieldNodes(child);
        }
        return count;
    }

    /**
     * Extract null counts from Arrow FlatBuffer metadata bytes.
     *
     * @param arrowMetadataBytes the Arrow FlatBuffer metadata (without IPC continuation/size
     *     header)
     * @param fieldNodeMapping pre-computed mapping from stats index to FieldNode index
     * @return null counts array aligned with statsIndexMapping positions
     */
    static Long[] extractNullCounts(byte[] arrowMetadataBytes, int[] fieldNodeMapping) {
        ByteBuffer buffer = ByteBuffer.wrap(arrowMetadataBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        Message message = Message.getRootAsMessage(buffer);
        RecordBatch recordBatch = (RecordBatch) message.header(new RecordBatch());
        if (recordBatch == null) {
            throw new IllegalStateException(
                    "Arrow metadata does not contain a RecordBatch header (type="
                            + message.headerType()
                            + ")");
        }

        FieldNode node = new FieldNode();
        Long[] nullCounts = new Long[fieldNodeMapping.length];
        for (int i = 0; i < fieldNodeMapping.length; i++) {
            int nodeIndex = fieldNodeMapping[i];
            if (nodeIndex < 0 || nodeIndex >= recordBatch.nodesLength()) {
                throw new IllegalStateException(
                        "FieldNode index "
                                + nodeIndex
                                + " out of range [0, "
                                + recordBatch.nodesLength()
                                + ")");
            }
            recordBatch.nodes(node, nodeIndex);
            nullCounts[i] = node.nullCount();
        }
        return nullCounts;
    }

    /** Cache key combining schemaId and statsIndexMapping. */
    static final class FieldNodeMappingKey {
        private final int schemaId;
        private final int[] statsIndexMapping;

        FieldNodeMappingKey(int schemaId, int[] statsIndexMapping) {
            this.schemaId = schemaId;
            this.statsIndexMapping = statsIndexMapping;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof FieldNodeMappingKey)) {
                return false;
            }
            FieldNodeMappingKey that = (FieldNodeMappingKey) o;
            return schemaId == that.schemaId
                    && Arrays.equals(statsIndexMapping, that.statsIndexMapping);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaId, Arrays.hashCode(statsIndexMapping));
        }
    }
}
