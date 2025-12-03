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

package org.apache.fluss.row.decode;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;

/**
 * A decoder to read {@link BinaryRow binary format InternalRow} from a byte array or memory segment
 * of a value record {@link ValueRecord}.
 *
 * @since 0.2
 */
@PublicEvolving
public interface RowDecoder {

    /** Create a {@link RowDecoder} for to decode {@link InternalRow} from a byte array. */
    static RowDecoder create(KvFormat kvFormat, DataType[] fieldDataTypes) {
        if (kvFormat == KvFormat.COMPACTED) {
            return new CompactedRowDecoder(fieldDataTypes);
        } else if (kvFormat == KvFormat.INDEXED) {
            return new IndexedRowDecoder(fieldDataTypes);
        } else {
            throw new IllegalArgumentException("Unsupported kv format: " + kvFormat);
        }
    }

    /** Decode the byte array to {@link BinaryRow}. */
    BinaryRow decode(byte[] values);

    /**
     * Decode the bytes in the memory segment to {@link BinaryRow}.
     *
     * @param segment the memory segment to read.
     * @param offset the offset in the memory segment to read from.
     * @param sizeInBytes the total size in bytes to read.
     */
    BinaryRow decode(MemorySegment segment, int offset, int sizeInBytes);
}
