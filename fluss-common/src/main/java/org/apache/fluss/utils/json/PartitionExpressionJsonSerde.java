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

package org.apache.fluss.utils.json;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionTransform;
import org.apache.fluss.metadata.TransformType;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.time.ZoneId;

/** Json serializer and deserializer for {@link PartitionExpression}. */
@Internal
public class PartitionExpressionJsonSerde
        implements JsonSerializer<PartitionExpression>, JsonDeserializer<PartitionExpression> {

    public static final PartitionExpressionJsonSerde INSTANCE = new PartitionExpressionJsonSerde();

    public static final String PARTITION_EXPRESSIONS_NAME = "partition_expressions";

    private static final String VIRTUAL_PARTITION_SPEC_KEY_NAME = "virtual_partition_spec_key";
    private static final String TRANSFORM_NAME = "transform";
    private static final String TYPE_NAME = "type";
    private static final String SOURCE_COLUMN_NAME = "source_column";
    private static final String UNIT_NAME = "unit";
    private static final String TIME_ZONE_NAME = "time_zone";
    private static final String DATE_TRUNC_TYPE = "date_trunc";

    @Override
    public void serialize(PartitionExpression partitionExpression, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        if (partitionExpression.getVirtualPartitionSpecKey().isPresent()) {
            generator.writeStringField(
                    VIRTUAL_PARTITION_SPEC_KEY_NAME,
                    partitionExpression.getVirtualPartitionSpecKey().get());
        }

        generator.writeFieldName(TRANSFORM_NAME);
        serializeTransform(partitionExpression.getTransform(), generator);
        generator.writeEndObject();
    }

    @Override
    public PartitionExpression deserialize(JsonNode node) {
        return deserializeInternal(node);
    }

    /**
     * Deserializes persisted partition-expression metadata.
     *
     * <p>Whether a time zone is required depends on the source column type and is validated once
     * the expression is combined with the table schema.
     */
    public PartitionExpression deserializeResolved(JsonNode node) {
        return deserializeInternal(node);
    }

    private PartitionExpression deserializeInternal(JsonNode node) {
        JsonNode virtualPartitionSpecKeyNode = node.get(VIRTUAL_PARTITION_SPEC_KEY_NAME);
        if (virtualPartitionSpecKeyNode == null) {
            throw new IllegalArgumentException(
                    "Partition expression must contain virtual_partition_spec_key.");
        }
        PartitionTransform transform = deserializeTransform(node.get(TRANSFORM_NAME));
        return PartitionExpression.of(virtualPartitionSpecKeyNode.asText(), transform);
    }

    private void serializeTransform(PartitionTransform transform, JsonGenerator generator)
            throws IOException {
        if (transform.getType() == TransformType.DATE_TRUNC) {
            DateTruncPartitionTransform dateTruncTransform =
                    (DateTruncPartitionTransform) transform;
            generator.writeStartObject();
            generator.writeStringField(TYPE_NAME, DATE_TRUNC_TYPE);
            generator.writeStringField(SOURCE_COLUMN_NAME, dateTruncTransform.getSourceColumn());
            generator.writeStringField(UNIT_NAME, dateTruncTransform.getTimeUnit().name());
            if (dateTruncTransform.getTimeZone().isPresent()) {
                generator.writeStringField(
                        TIME_ZONE_NAME, dateTruncTransform.getTimeZone().get().getId());
            }
            generator.writeEndObject();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported partition transform type: " + transform.getType());
        }
    }

    private PartitionTransform deserializeTransform(JsonNode node) {
        if (node == null) {
            throw new IllegalArgumentException("Partition expression must contain transform.");
        }
        String type = node.get(TYPE_NAME).asText();
        if (!DATE_TRUNC_TYPE.equals(type)) {
            throw new IllegalArgumentException("Unsupported partition transform type: " + type);
        }

        String sourceColumn = node.get(SOURCE_COLUMN_NAME).asText();
        AutoPartitionTimeUnit timeUnit =
                AutoPartitionTimeUnit.valueOf(node.get(UNIT_NAME).asText());
        JsonNode timeZoneNode = node.get(TIME_ZONE_NAME);
        if (timeZoneNode == null) {
            return DateTruncPartitionTransform.of(sourceColumn, timeUnit);
        }
        return DateTruncPartitionTransform.of(
                sourceColumn, timeUnit, ZoneId.of(timeZoneNode.asText()));
    }
}
