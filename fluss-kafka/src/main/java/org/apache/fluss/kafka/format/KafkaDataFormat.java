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

package org.apache.fluss.kafka.format;

import org.apache.fluss.annotation.Internal;

import java.util.Locale;

/** Supported interpretations of Kafka record key and value bytes. */
@Internal
public enum KafkaDataFormat {
    RAW("raw"),
    STRING("string"),
    JSON("json");

    /** Kafka topic config and Fluss custom property controlling the record key format. */
    public static final String KEY_FORMAT_CONFIG = "kafka.key.format";

    /** Kafka topic config and Fluss custom property controlling the record value format. */
    public static final String VALUE_FORMAT_CONFIG = "kafka.value.format";

    /** Fluss fields populated from the Kafka record key. */
    public static final String KEY_FIELDS_CONFIG = "kafka.key.fields";

    /** Strategy for deriving fields populated from the Kafka record value. */
    public static final String VALUE_FIELDS_INCLUDE_CONFIG = "kafka.value.fields-include";

    /** Nullable STRING column that captures JSON value fields absent from the table schema. */
    public static final String VALUE_RESCUE_COLUMN_CONFIG = "kafka.value.rescue-column";

    /** Fluss column populated from the Kafka record timestamp. */
    public static final String TIMESTAMP_COLUMN_CONFIG = "kafka.metadata.timestamp.column";

    /** Fluss column populated from the Kafka record headers. */
    public static final String HEADERS_COLUMN_CONFIG = "kafka.metadata.headers.column";

    private final String value;

    KafkaDataFormat(String value) {
        this.value = value;
    }

    /** Parses a topic config value. */
    public static KafkaDataFormat parse(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Kafka data format must not be null.");
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (KafkaDataFormat format : values()) {
            if (format.value.equals(normalized)) {
                return format;
            }
        }
        throw new IllegalArgumentException(
                "Unsupported Kafka data format '" + value + "'. Expected raw, string, or json.");
    }

    /** Returns the persisted topic config value. */
    public String value() {
        return value;
    }
}
