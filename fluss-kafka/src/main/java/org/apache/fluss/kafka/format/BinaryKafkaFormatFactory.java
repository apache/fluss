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
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.metadata.KafkaFieldProjection;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.types.BytesType;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

/** Factory for the Kafka binary byte format. */
@Internal
public final class BinaryKafkaFormatFactory implements KafkaFormatFactory {

    @Override
    public KafkaValueConversionType format() {
        return KafkaValueConversionType.BINARY;
    }

    @Override
    public KafkaFieldDecoder createDecoder(KafkaFieldProjection projection) {
        if (projection.size() != 1
                || !KafkaTableSchema.VALUE_COLUMN.equals(projection.nameAt(0))
                || !(projection.dataTypeAt(0) instanceof BytesType)) {
            throw new KafkaTopicSchemaException(
                    "Kafka binary format requires exactly one _kafka_value BYTES field.");
        }
        return bytes -> new Object[] {copyBytes(bytes)};
    }

    private static @Nullable byte[] copyBytes(@Nullable ByteBuffer bytes) {
        if (bytes == null) {
            return null;
        }
        byte[] copy = new byte[bytes.remaining()];
        bytes.duplicate().get(copy);
        return copy;
    }
}
