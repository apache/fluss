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
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Resolved Kafka key, value, and metadata mapping for one Fluss table schema. */
@Internal
public final class KafkaTopicSchema {

    private final RowType rowType;
    private final @Nullable KafkaDataFormat keyFormat;
    private final KafkaFieldProjection keyProjection;
    private final KafkaDataFormat valueFormat;
    private final KafkaFieldProjection valueProjection;
    private final @Nullable String valueRescueColumn;
    private final int timestampPosition;
    private final int headersPosition;

    /** Creates a resolved Kafka topic schema. */
    public KafkaTopicSchema(
            RowType rowType,
            @Nullable KafkaDataFormat keyFormat,
            KafkaFieldProjection keyProjection,
            KafkaDataFormat valueFormat,
            KafkaFieldProjection valueProjection,
            @Nullable String valueRescueColumn,
            int timestampPosition,
            int headersPosition) {
        this.rowType = checkNotNull(rowType);
        this.keyFormat = keyFormat;
        this.keyProjection = checkNotNull(keyProjection);
        this.valueFormat = checkNotNull(valueFormat);
        this.valueProjection = checkNotNull(valueProjection);
        this.valueRescueColumn = valueRescueColumn;
        this.timestampPosition = timestampPosition;
        this.headersPosition = headersPosition;
    }

    /** Returns the physical Fluss row type. */
    public RowType rowType() {
        return rowType;
    }

    /** Returns the key format, or null when the Kafka key is not mapped. */
    public @Nullable KafkaDataFormat keyFormat() {
        return keyFormat;
    }

    /** Returns the key field projection. */
    public KafkaFieldProjection keyProjection() {
        return keyProjection;
    }

    /** Returns the value format. */
    public KafkaDataFormat valueFormat() {
        return valueFormat;
    }

    /** Returns the value field projection. */
    public KafkaFieldProjection valueProjection() {
        return valueProjection;
    }

    /** Returns the JSON value rescue column, or null when strict unknown-field handling is used. */
    public @Nullable String valueRescueColumn() {
        return valueRescueColumn;
    }

    /** Returns the timestamp physical position, or -1 when it is not mapped. */
    public int timestampPosition() {
        return timestampPosition;
    }

    /** Returns the headers physical position, or -1 when they are not mapped. */
    public int headersPosition() {
        return headersPosition;
    }
}
