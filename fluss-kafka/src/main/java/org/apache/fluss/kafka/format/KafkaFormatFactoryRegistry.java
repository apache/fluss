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

package org.apache.fluss.kafka.format;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.format.json.JsonKafkaFormatFactory;
import org.apache.fluss.kafka.schema.KafkaFieldProjection;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;

import javax.annotation.Nullable;

import java.util.EnumMap;
import java.util.Map;

/** Registry of built-in Kafka record format factories. */
@Internal
public final class KafkaFormatFactoryRegistry {

    private final Map<KafkaDataFormat, KafkaFormatFactory> factories;

    /** Creates a registry containing all built-in formats. */
    public KafkaFormatFactoryRegistry() {
        factories = new EnumMap<>(KafkaDataFormat.class);
        register(new RawKafkaFormatFactory());
        register(new StringKafkaFormatFactory());
        register(new JsonKafkaFormatFactory());
    }

    /** Creates a decoder for the requested format and projection. */
    public KafkaFieldDecoder createDecoder(
            KafkaDataFormat format,
            KafkaFieldProjection projection,
            @Nullable String valueRescueColumn) {
        KafkaFormatFactory factory = factories.get(format);
        if (factory == null) {
            throw new KafkaTopicSchemaException(
                    "No Kafka format factory is registered for '" + format.value() + "'.");
        }
        return factory.createDecoder(projection, valueRescueColumn);
    }

    private void register(KafkaFormatFactory factory) {
        factories.put(factory.format(), factory);
    }
}
