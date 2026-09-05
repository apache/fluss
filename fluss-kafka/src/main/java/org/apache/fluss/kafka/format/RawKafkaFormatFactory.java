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
import org.apache.fluss.kafka.schema.KafkaFieldProjection;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.types.BytesType;

import javax.annotation.Nullable;

/** Factory for the Kafka raw byte format. */
@Internal
public final class RawKafkaFormatFactory implements KafkaFormatFactory {

    @Override
    public KafkaDataFormat format() {
        return KafkaDataFormat.RAW;
    }

    @Override
    public KafkaFieldDecoder createDecoder(
            KafkaFieldProjection projection, @Nullable String valueRescueColumn) {
        if (valueRescueColumn != null) {
            throw new KafkaTopicSchemaException(
                    "Kafka value rescue column is only supported for JSON format.");
        }
        if (projection.size() != 1 || !(projection.dataTypeAt(0) instanceof BytesType)) {
            throw new KafkaTopicSchemaException(
                    "Kafka raw format requires exactly one BYTES field.");
        }
        return bytes -> new Object[] {bytes};
    }
}
