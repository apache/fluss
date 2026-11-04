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

package org.apache.fluss.kafka.schema;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.TableDescriptor;

/** Resolves the same persisted Kafka contract that native DDL validates. */
@Internal
public final class KafkaTopicSchemaResolver {

    /** Returns the table's explicit opt-in flag without inferring it from schema compatibility. */
    public boolean isEnabled(TableDescriptor table) {
        try {
            return KafkaTableSchema.isEnabled(table);
        } catch (IllegalArgumentException e) {
            throw new KafkaTopicSchemaException(e.getMessage());
        }
    }

    /** Resolves one enabled table's contract; invalid enabled tables remain visible as errors. */
    public KafkaTableSchema resolve(TableDescriptor table) {
        try {
            return KafkaTableSchema.resolve(table);
        } catch (InvalidTableException | IllegalArgumentException e) {
            throw new KafkaTopicSchemaException(e.getMessage());
        }
    }
}
