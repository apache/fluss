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
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** An encoder to encode {@link BinaryRow} with a schema id as value to be stored in kv store. */
public final class ValueEncoder {

    private static final KvValueLayout KV_FORMAT_VERSION_2_LAYOUT =
            KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_2);
    private static final ValueHeaderWriter SCHEMA_ONLY_HEADER_WRITER =
            (layout, schemaId, ignoredRow, target) -> layout.writeSchemaId(target, schemaId);

    private final KvValueLayout kvValueLayout;
    private final ValueHeaderWriter valueHeaderWriter;

    private ValueEncoder(KvValueLayout kvValueLayout, ValueHeaderWriter valueHeaderWriter) {
        this.kvValueLayout = kvValueLayout;
        this.valueHeaderWriter = valueHeaderWriter;
    }

    /** Creates a version-aware value encoder for the table KV format version. */
    public static ValueEncoder forKvFormatVersion(
            int kvFormatVersion, @Nullable ToLongFunction<BinaryRow> valueTagProvider) {
        KvValueLayout kvValueLayout = KvValueLayout.forKvFormatVersion(kvFormatVersion);
        switch (kvValueLayout.headerKind()) {
            case SCHEMA_ONLY:
                if (valueTagProvider != null) {
                    throw new IllegalArgumentException(
                            "valueTagProvider must be null for a KV value layout without a value tag.");
                }
                return new ValueEncoder(kvValueLayout, SCHEMA_ONLY_HEADER_WRITER);
            case SCHEMA_WITH_VALUE_TAG:
                if (valueTagProvider == null) {
                    throw new IllegalArgumentException(
                            "valueTagProvider must be non-null for a KV value layout with a value tag.");
                }
                return new ValueEncoder(kvValueLayout, valueTagHeaderWriter(valueTagProvider));
            default:
                throw new IllegalStateException(
                        "Unsupported KV value header kind " + kvValueLayout.headerKind() + ".");
        }
    }

    /**
     * Creates a value encoder with the same KV format version and a different value tag provider.
     */
    public ValueEncoder withValueTagProvider(ToLongFunction<BinaryRow> valueTagProvider) {
        checkState(
                kvValueLayout.headerKind() == KvValueLayout.HeaderKind.SCHEMA_WITH_VALUE_TAG,
                "valueTagProvider can only be replaced for a KV value layout with a value tag.");
        checkNotNull(valueTagProvider, "valueTagProvider must not be null.");
        return new ValueEncoder(kvValueLayout, valueTagHeaderWriter(valueTagProvider));
    }

    /** Returns whether this encoder writes an internal value tag before the row bytes. */
    public boolean hasValueTag() {
        return kvValueLayout.hasValueTag();
    }

    /** Encodes a binary value using the table KV format version bound to this encoder. */
    public byte[] encodeValue(BinaryValue value) {
        return encodeValue(kvValueLayout, valueHeaderWriter, value.schemaId, value.row);
    }

    /**
     * Encode the {@code row} with a {@code schemaId} to a byte array value to be expected persisted
     * to kv store.
     *
     * @param schemaId the schema id of the row
     * @param row the row to encode
     */
    public static byte[] encodeValue(short schemaId, BinaryRow row) {
        return encodeValue(KV_FORMAT_VERSION_2_LAYOUT, SCHEMA_ONLY_HEADER_WRITER, schemaId, row);
    }

    private static byte[] encodeValue(
            KvValueLayout kvValueLayout,
            ValueHeaderWriter valueHeaderWriter,
            short schemaId,
            BinaryRow row) {
        int rowPayloadOffset = kvValueLayout.rowPayloadOffset();
        byte[] values = new byte[rowPayloadOffset + row.getSizeInBytes()];
        valueHeaderWriter.writeHeader(kvValueLayout, schemaId, row, values);
        row.copyTo(values, rowPayloadOffset);
        return values;
    }

    private static ValueHeaderWriter valueTagHeaderWriter(
            ToLongFunction<BinaryRow> valueTagProvider) {
        return (layout, schemaId, row, target) -> {
            layout.writeSchemaId(target, schemaId);
            layout.writeValueTag(target, valueTagProvider.applyAsLong(row));
        };
    }

    @FunctionalInterface
    private interface ValueHeaderWriter {
        void writeHeader(KvValueLayout layout, short schemaId, BinaryRow row, byte[] target);
    }
}
