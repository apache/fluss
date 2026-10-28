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
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.format.KafkaFormatFactoryRegistry;
import org.apache.fluss.metadata.KafkaTableSchema;

import javax.annotation.concurrent.ThreadSafe;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Immutable conversion plan shared across requests without retaining records or Arrow resources.
 */
@Internal
@ThreadSafe
final class KafkaTopicWritePlan {
    private final KafkaFieldDecoder valueDecoder;
    private final KafkaRowAssembler rowAssembler;

    KafkaTopicWritePlan(KafkaTableSchema topicSchema, KafkaFormatFactoryRegistry formats) {
        this.valueDecoder = formats.createDecoder(checkNotNull(topicSchema));
        this.rowAssembler = new KafkaRowAssembler(topicSchema);
    }

    KafkaFieldDecoder valueDecoder() {
        return valueDecoder;
    }

    KafkaRowAssembler rowAssembler() {
        return rowAssembler;
    }
}
