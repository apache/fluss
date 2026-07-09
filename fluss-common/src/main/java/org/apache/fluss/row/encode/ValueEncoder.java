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

package org.apache.fluss.row.encode;

import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;

import javax.annotation.Nullable;

import java.util.function.ToLongFunction;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An encoder to encode {@link BinaryRow} with a schema id as value to be stored in kv store. */
public class ValueEncoder {

    private final ValueLayout valueLayout;
    @Nullable private final ToLongFunction<BinaryRow> valueTimestampProvider;

    private ValueEncoder(
            ValueLayout valueLayout, @Nullable ToLongFunction<BinaryRow> valueTimestampProvider) {
        this.valueLayout = valueLayout;
        this.valueTimestampProvider = valueTimestampProvider;
    }

    /** Creates a version-aware value encoder for the table KV format version. */
    public static ValueEncoder forVersion(
            int kvFormatVersion, @Nullable ToLongFunction<BinaryRow> valueTimestampProvider) {
        ValueLayout valueLayout = ValueLayout.forVersion(kvFormatVersion);
        if (valueLayout.hasValueTimestamp() && valueTimestampProvider == null) {
            throw new IllegalArgumentException(
                    "valueTimestampProvider must be non-null for a value layout with timestamp.");
        }
        if (!valueLayout.hasValueTimestamp() && valueTimestampProvider != null) {
            throw new IllegalArgumentException(
                    "valueTimestampProvider must be null for a value layout without timestamp.");
        }
        return new ValueEncoder(valueLayout, valueTimestampProvider);
    }

    /**
     * Creates a value encoder with the same KV format version and a different timestamp provider.
     */
    public ValueEncoder withTimestampProvider(ToLongFunction<BinaryRow> timestampProvider) {
        if (!valueLayout.hasValueTimestamp()) {
            throw new IllegalStateException(
                    "timestampProvider can only be replaced for a value layout with timestamp.");
        }
        checkNotNull(timestampProvider, "timestampProvider must not be null.");
        return new ValueEncoder(valueLayout, timestampProvider);
    }

    /** Returns whether this encoder writes an internal value timestamp before the row bytes. */
    public boolean hasValueTimestamp() {
        return valueLayout.hasValueTimestamp();
    }

    /** Creates a binary value using the table KV format version bound to this encoder. */
    public BinaryValue createValue(short schemaId, BinaryRow row) {
        if (valueLayout.hasValueTimestamp()) {
            return new BinaryValue(
                    schemaId,
                    checkNotNull(valueTimestampProvider, "valueTimestampProvider must not be null.")
                            .applyAsLong(row),
                    row);
        }
        return new BinaryValue(schemaId, row);
    }

    /**
     * Encode the {@code row} with a {@code schemaId} to a byte array value to be expected persisted
     * to kv store.
     *
     * @param schemaId the schema id of the row
     * @param row the row to encode
     */
    public static byte[] encodeValue(short schemaId, BinaryRow row) {
        ValueLayout valueLayout = ValueLayout.forVersion(KV_FORMAT_VERSION_2);
        byte[] values = new byte[valueLayout.rowOffset() + row.getSizeInBytes()];
        valueLayout.writeSchemaId(values, schemaId);
        row.copyTo(values, valueLayout.rowOffset());
        return values;
    }

    /**
     * Encode the {@code row} with a {@code schemaId} and value timestamp to a byte array value to
     * be expected persisted to kv store.
     *
     * @param schemaId the schema id of the row
     * @param valueTimestampMs the value timestamp in milliseconds
     * @param row the row to encode
     */
    public static byte[] encodeValue(short schemaId, long valueTimestampMs, BinaryRow row) {
        ValueLayout valueLayout = ValueLayout.forVersion(KV_FORMAT_VERSION_3);
        byte[] values = new byte[valueLayout.rowOffset() + row.getSizeInBytes()];
        valueLayout.writeSchemaId(values, schemaId);
        valueLayout.writeValueTimestamp(values, valueTimestampMs);
        row.copyTo(values, valueLayout.rowOffset());
        return values;
    }
}
