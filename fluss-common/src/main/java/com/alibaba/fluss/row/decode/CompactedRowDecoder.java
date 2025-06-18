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

package com.alibaba.fluss.row.decode;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.compacted.CompactedRowDeserializer;
import com.alibaba.fluss.types.DataType;

/** A decoder to decode {@link CompactedRow} from a byte array or memory segment. */
public class CompactedRowDecoder implements RowDecoder {

    private final DataType[] fieldDataTypes;
    private final CompactedRowDeserializer deserializer;

    public CompactedRowDecoder(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
        this.deserializer = new CompactedRowDeserializer(fieldDataTypes);
    }

    @Override
    public CompactedRow decode(byte[] values) {
        return CompactedRow.from(fieldDataTypes, values, deserializer);
    }

    @Override
    public CompactedRow decode(MemorySegment segment, int offset, int sizeInBytes) {
        CompactedRow compactedRow = new CompactedRow(fieldDataTypes.length, deserializer);
        compactedRow.pointTo(segment, offset, sizeInBytes);
        return compactedRow;
    }
}
