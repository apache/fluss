/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.format.json;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.KafkaValueConversionLayout;
import org.apache.fluss.kafka.format.KafkaDecodedValue;
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaFieldProjection;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decodes a strict UTF-8 JSON object according to the projected Fluss fields.
 *
 * <p>Unpaired surrogate escapes follow Jackson parsing and Fluss's default string encoding.
 *
 * <p>Jackson's node tree preserves ordinary decimal precision but normalizes signed zero. Extreme
 * exponents may be rejected or lose numeric fidelity during parsing, including in rescued JSON.
 */
@Internal
@ThreadSafe
public final class JsonKafkaFieldDecoder implements KafkaFieldDecoder {
    // The JSON root and a container at business depth zero add two levels to schema depth.
    // The nested physical ROW represents the JSON root and adds no extra level.
    static final int MAX_JSON_NESTING_DEPTH = KafkaTableSchema.MAX_JSON_SCHEMA_NESTING_DEPTH + 2;
    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private final KafkaFieldProjection projection;
    private final boolean rescueEnabled;
    private final boolean nested;
    private final Set<String> projectedFields = new HashSet<>();
    private final JsonToFlussConverter[] converters;

    /** Creates a decoder from the schema validated by the shared Kafka table contract. */
    public JsonKafkaFieldDecoder(KafkaTableSchema schema) {
        rescueEnabled = schema.rescuePosition() >= 0;
        nested = schema.valueConversionLayout() == KafkaValueConversionLayout.NESTED;
        if (nested) {
            RowType nestedType = (RowType) schema.valueProjection().dataTypeAt(0);
            List<Integer> positions = new ArrayList<>();
            for (int i = 0; i < nestedType.getFieldCount(); i++) {
                positions.add(i);
            }
            projection = new KafkaFieldProjection(nestedType, positions);
        } else {
            projection = schema.valueProjection();
        }
        converters = new JsonToFlussConverter[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            projectedFields.add(projection.nameAt(i));
            converters[i] = JsonToFlussConverters.create(projection.dataTypeAt(i));
        }
    }

    @Override
    public KafkaDecodedValue decode(@Nullable ByteBuffer bytes) {
        if (bytes == null) {
            return new KafkaDecodedValue(new Object[nested ? 1 : projection.size()], null);
        }
        try (Reader reader =
                        new InputStreamReader(
                                new ByteBufferBackedInputStream(bytes.duplicate()),
                                StandardCharsets.UTF_8
                                        .newDecoder()
                                        .onMalformedInput(CodingErrorAction.REPORT)
                                        .onUnmappableCharacter(CodingErrorAction.REPORT));
                JsonParser parser = OBJECT_MAPPER.getFactory().createParser(reader)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new KafkaRecordEncodingException(
                        "Kafka JSON record value must have an object root.");
            }
            JsonNode root = OBJECT_MAPPER.readTree(parser);
            if (parser.nextToken() != null) {
                throw new KafkaRecordEncodingException(
                        "Kafka record value is not valid strict UTF-8 JSON: trailing content.");
            }
            ObjectNode rescuedFields = rescueEnabled ? OBJECT_MAPPER.createObjectNode() : null;
            Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String name = field.getKey();
                boolean reserved = !nested && KafkaTableSchema.isReservedName(name);
                if (reserved || !projectedFields.contains(name)) {
                    if (!rescueEnabled) {
                        throw new KafkaRecordEncodingException(
                                "Invalid Kafka record value at "
                                        + JsonPath.field(JsonPath.ROOT, name)
                                        + (reserved ? ": reserved field." : ": unknown field."));
                    }
                    rescuedFields.set(name, field.getValue());
                }
            }
            Object[] values = new Object[projection.size()];
            for (int i = 0; i < projection.size(); i++) {
                String name = projection.nameAt(i);
                values[i] =
                        converters[i].convert(
                                root.get(name),
                                JsonPath.field(JsonPath.ROOT, name),
                                rescueEnabled ? rescued -> rescuedFields.set(name, rescued) : null);
            }
            return new KafkaDecodedValue(
                    nested ? new Object[] {GenericRow.of(values)} : values,
                    rescuedFields == null || rescuedFields.isEmpty()
                            ? null
                            : rescuedFields.toString());
        } catch (KafkaRecordEncodingException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new KafkaRecordEncodingException(
                    "Kafka record value is not valid strict UTF-8 JSON.", e);
        }
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.getFactory()
                .setStreamReadConstraints(
                        StreamReadConstraints.builder()
                                .maxNestingDepth(MAX_JSON_NESTING_DEPTH)
                                .maxNumberLength(StreamReadConstraints.DEFAULT_MAX_NUM_LEN)
                                .maxStringLength(StreamReadConstraints.DEFAULT_MAX_STRING_LEN)
                                .build());
        mapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        // TODO (#4863): preserve signed zero and extreme exponent/rescue fidelity when reading
        // scalar tokens directly. Keep this PR on Jackson's complete-tree parsing path.
        return mapper;
    }
}
