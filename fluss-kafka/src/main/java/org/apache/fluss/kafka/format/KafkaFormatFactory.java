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
import org.apache.fluss.config.KafkaValueConversionLayout;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.metadata.KafkaFieldProjection;
import org.apache.fluss.metadata.KafkaTableSchema;

import javax.annotation.Nullable;

/** Creates a decoder for one Kafka value format and Fluss field projection. */
@Internal
public interface KafkaFormatFactory {

    /** Returns the format implemented by this factory. */
    KafkaValueConversionType format();

    /** Creates and validates a decoder for the projection. */
    KafkaFieldDecoder createDecoder(KafkaFieldProjection projection);

    /** Creates a decoder from the validated fixed-column contract. */
    default KafkaFieldDecoder createDecoder(KafkaTableSchema schema) {
        return createDecoder(schema.valueProjection(), schema.valueConversionLayout());
    }

    /** Creates a decoder with the JSON layout, or no layout for binary/string values. */
    default KafkaFieldDecoder createDecoder(
            KafkaFieldProjection projection, @Nullable KafkaValueConversionLayout layout) {
        return createDecoder(projection);
    }
}
