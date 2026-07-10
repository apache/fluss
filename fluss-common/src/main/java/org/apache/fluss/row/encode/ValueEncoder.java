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

    private final KvValueLayout kvValueLayout;
    @Nullable private final ToLongFunction<BinaryRow> valueTagProvider;

    private ValueEncoder(
            KvValueLayout kvValueLayout, @Nullable ToLongFunction<BinaryRow> valueTagProvider) {
        this.kvValueLayout = kvValueLayout;
        this.valueTagProvider = valueTagProvider;
    }

    /** Creates a version-aware value encoder for the table KV format version. */
    public static ValueEncoder forKvFormatVersion(
            int kvFormatVersion, @Nullable ToLongFunction<BinaryRow> valueTagProvider) {
        KvValueLayout kvValueLayout = KvValueLayout.forKvFormatVersion(kvFormatVersion);
        if (kvValueLayout.hasValueTag() && valueTagProvider == null) {
            throw new IllegalArgumentException(
                    "valueTagProvider must be non-null for a KV value layout with a value tag.");
        }
        if (!kvValueLayout.hasValueTag() && valueTagProvider != null) {
            throw new IllegalArgumentException(
                    "valueTagProvider must be null for a KV value layout without a value tag.");
        }
        return new ValueEncoder(kvValueLayout, valueTagProvider);
    }

    /**
     * Creates a value encoder with the same KV format version and a different value tag provider.
     */
    public ValueEncoder withValueTagProvider(ToLongFunction<BinaryRow> valueTagProvider) {
        if (!kvValueLayout.hasValueTag()) {
            throw new IllegalStateException(
                    "valueTagProvider can only be replaced for a KV value layout with a value tag.");
        }
        checkNotNull(valueTagProvider, "valueTagProvider must not be null.");
        return new ValueEncoder(kvValueLayout, valueTagProvider);
    }

    /** Returns whether this encoder writes an internal value tag before the row bytes. */
    public boolean hasValueTag() {
        return kvValueLayout.hasValueTag();
    }

    /** Creates a binary value using the table KV format version bound to this encoder. */
    public BinaryValue createValue(short schemaId, BinaryRow row) {
        if (kvValueLayout.hasValueTag()) {
            return new BinaryValue(
                    schemaId,
                    checkNotNull(valueTagProvider, "valueTagProvider must not be null.")
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
        KvValueLayout kvValueLayout = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_2);
        byte[] values = new byte[kvValueLayout.rowPayloadOffset() + row.getSizeInBytes()];
        kvValueLayout.writeSchemaId(values, schemaId);
        row.copyTo(values, kvValueLayout.rowPayloadOffset());
        return values;
    }

    /**
     * Encode the {@code row} with a {@code schemaId} and value tag to a byte array value to be
     * expected persisted to kv store.
     *
     * @param schemaId the schema id of the row
     * @param valueTag the opaque value tag
     * @param row the row to encode
     */
    public static byte[] encodeValueWithTag(short schemaId, long valueTag, BinaryRow row) {
        KvValueLayout kvValueLayout = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3);
        byte[] values = new byte[kvValueLayout.rowPayloadOffset() + row.getSizeInBytes()];
        kvValueLayout.writeSchemaId(values, schemaId);
        kvValueLayout.writeValueTag(values, valueTag);
        row.copyTo(values, kvValueLayout.rowPayloadOffset());
        return values;
    }
}
