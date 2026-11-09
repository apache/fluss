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

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Decoded value fields and optional JSON rescue for immediate synchronous row encoding. */
@Internal
@NotThreadSafe
public final class KafkaDecodedValue {
    private final Object[] valueFields;
    @Nullable private final String rescue;

    /** Wraps the projected fields without copying their potentially borrowed storage. */
    public KafkaDecodedValue(Object[] valueFields, @Nullable String rescue) {
        this.valueFields = checkNotNull(valueFields);
        this.rescue = rescue;
    }

    /** Returns fields in value-projection order, excluding system columns. */
    public Object[] valueFields() {
        return valueFields;
    }

    /** Returns rescued JSON, or null when no fields were rescued. */
    @Nullable
    public String rescue() {
        return rescue;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof KafkaDecodedValue)) {
            return false;
        }
        KafkaDecodedValue that = (KafkaDecodedValue) other;
        return Arrays.deepEquals(valueFields, that.valueFields)
                && Objects.equals(rescue, that.rescue);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.deepHashCode(valueFields) + Objects.hashCode(rescue);
    }

    @Override
    public String toString() {
        return "KafkaDecodedValue{valueFields="
                + Arrays.deepToString(valueFields)
                + ", rescue="
                + rescue
                + '}';
    }
}
