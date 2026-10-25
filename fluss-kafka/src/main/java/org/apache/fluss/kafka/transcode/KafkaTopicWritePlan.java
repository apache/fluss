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

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.metadata.KafkaTableSchema;

import javax.annotation.concurrent.ThreadSafe;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Immutable conversion plan shared across requests without retaining records or Arrow resources.
 */
@Internal
@ThreadSafe
final class KafkaTopicWritePlan {
    private final KafkaTableSchema topicSchema;
    private final KafkaRowAssembler rowAssembler;

    KafkaTopicWritePlan(KafkaTableSchema topicSchema) {
        this.topicSchema = checkNotNull(topicSchema);
        // Validate supported value formats when compiling the plan.
        if (topicSchema.valueConversionType() != KafkaValueConversionType.BINARY
                && topicSchema.valueConversionType() != KafkaValueConversionType.STRING) {
            throw new KafkaRecordEncodingException(
                    "Unsupported Kafka value conversion type: "
                            + topicSchema.valueConversionType());
        }
        this.rowAssembler = new KafkaRowAssembler(topicSchema);
    }

    KafkaTableSchema topicSchema() {
        return topicSchema;
    }

    KafkaRowAssembler rowAssembler() {
        return rowAssembler;
    }
}
