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

import java.util.function.ToLongFunction;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An encoder to encode {@link BinaryRow} with a schema id as value to be stored in kv store. */
public final class ValueEncoder {

    private static final ValueEncoder PLAIN_ENCODER = new ValueEncoder(KvValueLayout.PLAIN, null);

    private final KvValueLayout kvValueLayout;
    private final ToLongFunction<BinaryRow> valueTagProvider;

    private ValueEncoder(KvValueLayout kvValueLayout, ToLongFunction<BinaryRow> valueTagProvider) {
        this.kvValueLayout = kvValueLayout;
        this.valueTagProvider = valueTagProvider;
    }

    /** Returns an encoder for a layout without an internal value tag. */
    public static ValueEncoder forLayout(KvValueLayout kvValueLayout) {
        checkNotNull(kvValueLayout, "kvValueLayout must not be null.");
        if (kvValueLayout != KvValueLayout.PLAIN) {
            throw new IllegalArgumentException(
                    "A value tag provider is required for this KV value layout.");
        }
        return PLAIN_ENCODER;
    }

    /** Returns an encoder for a layout with an internal value tag. */
    public static ValueEncoder forLayout(
            KvValueLayout kvValueLayout, ToLongFunction<BinaryRow> valueTagProvider) {
        checkNotNull(kvValueLayout, "kvValueLayout must not be null.");
        checkNotNull(valueTagProvider, "valueTagProvider must not be null.");
        if (!kvValueLayout.hasValueTag()) {
            throw new IllegalArgumentException(
                    "A value tag provider is not supported for this KV value layout.");
        }
        return new ValueEncoder(kvValueLayout, valueTagProvider);
    }

    /** Returns whether this encoder writes an internal value tag before the row bytes. */
    public boolean hasValueTag() {
        return kvValueLayout.hasValueTag();
    }

    /** Encodes a binary value using the layout bound to this encoder. */
    public byte[] encodeValue(BinaryValue value) {
        int rowPayloadOffset = kvValueLayout.rowPayloadOffset();
        byte[] values = new byte[rowPayloadOffset + value.row.getSizeInBytes()];
        kvValueLayout.writeSchemaId(values, value.schemaId);
        if (valueTagProvider != null) {
            kvValueLayout.writeValueTag(values, valueTagProvider.applyAsLong(value.row));
        }
        value.row.copyTo(values, rowPayloadOffset);
        return values;
    }

    /** Encodes a plain binary value for callers that do not select a layout explicitly. */
    public static byte[] encodeValue(short schemaId, BinaryRow row) {
        return PLAIN_ENCODER.encodeValue(new BinaryValue(schemaId, row));
    }
}
