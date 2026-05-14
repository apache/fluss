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

package org.apache.fluss.row.encode.hudi;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link KeyEncoder} to follow Hudi's encoding strategy.
 *
 * <p>The encoded bytes are a 4-byte big-endian representation of {@code
 * List<String>.hashCode()} over the stringified key fields, which matches the way
 * Hudi's {@code BucketIdentifier} hashes a record key. Null fields are replaced by
 * {@link #NULL_RECORDKEY_PLACEHOLDER} so that an explicit null and the literal
 * string {@code "null"} no longer collide in the hash space.
 */
public class HudiKeyEncoder implements KeyEncoder {

    /**
     * Placeholder used to represent a {@code null} key field when computing the
     * record-key hash. It is intentionally aligned with Hudi's
     * {@code KeyGenUtils.NULL_RECORDKEY_PLACEHOLDER} so that the resulting bucket id
     * stays identical to what Hudi would compute on its side.
     */
    public static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";

    private final InternalRow.FieldGetter[] fieldGetters;

    public HudiKeyEncoder(RowType rowType, List<String> keys) {
        // for getting key fields out of fluss internal row
        fieldGetters = new InternalRow.FieldGetter[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            int keyIndex = rowType.getFieldIndex(keys.get(i));
            DataType keyDataType = rowType.getTypeAt(keyIndex);
            fieldGetters[i] = InternalRow.createFieldGetter(keyDataType, keyIndex);
        }
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        // Build the same string list that Hudi would build out of a record key, so the
        // resulting List#hashCode() — and therefore the bucket id — match Hudi's own
        // BucketIdentifier#getBucketId.
        List<String> values = new ArrayList<>(fieldGetters.length);
        for (InternalRow.FieldGetter fieldGetter : fieldGetters) {
            Object value = fieldGetter.getFieldOrNull(row);
            values.add(stringifyForRecordKey(value));
        }
        int hashCode = values.hashCode();

        // 4-byte big-endian, decoded symmetrically by HudiBucketingFunction.
        return new byte[] {
            (byte) (hashCode >>> 24),
            (byte) (hashCode >>> 16),
            (byte) (hashCode >>> 8),
            (byte) hashCode
        };
    }

    private static String stringifyForRecordKey(Object value) {
        if (value == null) {
            return NULL_RECORDKEY_PLACEHOLDER;
        }
        if (value instanceof BinaryString) {
            return value.toString();
        }
        return String.valueOf(value);
    }
}