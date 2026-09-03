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
    STRING("string");

    /** Kafka topic config and Fluss custom property controlling the record key format. */
    public static final String KEY_FORMAT_CONFIG = "fluss.key.format";

    /** Kafka topic config and Fluss custom property controlling the record value format. */
    public static final String VALUE_FORMAT_CONFIG = "fluss.value.format";

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
                "Unsupported Kafka data format '" + value + "'. Expected raw or string.");
    }

    /** Returns the persisted topic config value. */
    public String value() {
        return value;
    }
}
