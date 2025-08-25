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

package org.apache.fluss.flink.source.deserializer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.flink.utils.FlussRowToJsonConverters;
import org.apache.fluss.flink.utils.TimestampFormat;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A deserialization schema that converts {@link LogRecord} objects to JSON strings.
 *
 * <p>This implementation serializes Fluss records into JSON strings, making it useful for
 * debugging, logging, or when the downstream processing requires string-based JSON data. The schema
 * preserves important metadata such as offset, timestamp, and change type along with the actual row
 * data.
 *
 * <p>The resulting JSON has the following structure:
 *
 * <pre>{@code
 * {
 *   "offset": <record_offset>,
 *   "timestamp": <record_timestamp>,
 *   "changeType": <APPEND_ONLY|INSERT|UPDATE_BEFORE|UPDATE_AFTER|DELETE>,
 *   "row": <string_representation_of_row>
 * }
 * }</pre>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * FlussSource<String> source = FlussSource.builder()
 *     .setDeserializationSchema(new JsonStringDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * @since 0.7
 */
@PublicEvolving
public class JsonStringDeserializationSchema implements FlussDeserializationSchema<String> {
    private static final long serialVersionUID = 1L;

    /**
     * Jackson ObjectMapper used for JSON serialization. Marked as transient because ObjectMapper is
     * not serializable and needs to be recreated in the open method.
     */
    private transient ObjectMapper objectMapper = new ObjectMapper();

    /** Reusable object node. */
    private transient ObjectNode node;

    /**
     * Reusable map for building the record representation before serializing to JSON. This avoids
     * creating a new Map for each record. Using LinkedHashMap to ensure a stable order of fields in
     * the JSON output.
     */
    private final Map<String, Object> recordMap = new LinkedHashMap<>(4);

    /**
     * Converter responsible for transforming Fluss row data into json. Initialized during {@link
     * #open(InitializationContext)}.
     */
    private transient FlussRowToJsonConverters.FlussRowToJsonConverter runtimeConverter;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    public JsonStringDeserializationSchema() {
        this(TimestampFormat.ISO_8601);
    }

    private JsonStringDeserializationSchema(TimestampFormat timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    /**
     * Initializes the JSON serialization mechanism.
     *
     * <p>This method creates a new ObjectMapper instance and configures it with:
     *
     * <ul>
     *   <li>JavaTimeModule for proper serialization of date/time objects
     *   <li>Configuration to render dates in ISO-8601 format rather than timestamps
     * </ul>
     *
     * @param context Contextual information for initialization (not used in this implementation)
     * @throws Exception if initialization fails
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        if (runtimeConverter == null) {
            this.runtimeConverter =
                    new FlussRowToJsonConverters(timestampFormat)
                            .createNullableConverter(context.getRowSchema());
        }
        objectMapper = new ObjectMapper();

        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    /**
     * Deserializes a {@link LogRecord} into a JSON {@link String}.
     *
     * <p>The method extracts key information from the record (offset, timestamp, change type, and
     * row data) and serializes it as a JSON string.
     *
     * @param record The Fluss LogRecord to deserialize
     * @return JSON string representation of the record
     * @throws Exception If JSON serialization fails
     */
    @Override
    public String deserialize(LogRecord record) throws Exception {
        recordMap.put("offset", record.logOffset());
        recordMap.put("timestamp", record.timestamp());
        recordMap.put("change_type", record.getChangeType().toString());

        if (node == null) {
            node = objectMapper.createObjectNode();
        }
        recordMap.put("row", runtimeConverter.convert(objectMapper, node, record.getRow()));

        return objectMapper.writeValueAsString(recordMap);
    }

    /**
     * Returns the TypeInformation for the produced {@link String} type.
     *
     * @return TypeInformation for String class
     */
    @Override
    public TypeInformation<String> getProducedType(RowType rowSchema) {
        return Types.STRING;
    }
}
