/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.decode;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.compacted.CompactedRowReader;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.List;

/**
 * A decoder to decode key bytes (encoded by {@link CompactedKeyEncoder}) back to {@link
 * InternalRow}.
 */
public class CompactedKeyDecoder implements KeyDecoder {

    private final DataType[] keyDataTypes;

    /**
     * Create a key decoder to decode the key bytes back to a row containing only key fields.
     *
     * @param rowType the row type used to locate key field positions
     * @param keys the key field names to decode
     */
    public static CompactedKeyDecoder createKeyDecoder(RowType rowType, List<String> keys) {
        int[] keyFieldPos = new int[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            keyFieldPos[i] = rowType.getFieldIndex(keys.get(i));
            if (keyFieldPos[i] == -1) {
                throw new IllegalArgumentException(
                        "Field " + keys.get(i) + " not found in row type " + rowType);
            }
        }
        return new CompactedKeyDecoder(rowType, keyFieldPos);
    }

    public CompactedKeyDecoder(RowType rowType, int[] keyFieldPos) {
        // for decoding key fields
        keyDataTypes = new DataType[keyFieldPos.length];
        for (int i = 0; i < keyFieldPos.length; i++) {
            keyDataTypes[i] = rowType.getTypeAt(keyFieldPos[i]).copy(false);
        }
    }

    /**
     * Decode the key bytes back to a row containing only key fields. Non-key fields are not
     * included in the returned row.
     *
     * @param keyBytes the key bytes encoded by {@link CompactedKeyEncoder}
     * @return a row containing only the decoded key fields
     */
    public InternalRow decodeKey(byte[] keyBytes) {
        // Decode key fields directly into a GenericRow matching the decoded key size
        CompactedRowReader compactedRowReader = new CompactedRowReader(0);
        compactedRowReader.pointTo(MemorySegment.wrap(keyBytes), 0, keyBytes.length);

        CompactedRowDeserializer compactedRowDeserializer =
                new CompactedRowDeserializer(keyDataTypes);
        GenericRow genericRow = new GenericRow(keyDataTypes.length);
        compactedRowDeserializer.deserialize(compactedRowReader, genericRow);
        return genericRow;
    }
}
