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

package com.alibaba.fluss.row.encode;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

/**
 * An encoder to write {@link BinaryRow binary format InternalRow}. It's used to write row
 * multi-times one by one. When writing a new row:
 *
 * <ol>
 *   <li>call method {@link #startNewRow()} to start the writing.
 *   <li>call method {@link #encodeField(int, Object)} to write the row's field.
 *   <li>call method {@link #finishRow()} to finish the writing and get the written row.
 * </ol>
 *
 * @since 0.2
 */
@PublicEvolving
public interface RowEncoder extends AutoCloseable {

    /**
     * Create a {@link RowEncoder} for encoding Java objects to a {@link MemorySegment} backed
     * {@link InternalRow}.
     */
    static RowEncoder create(KvFormat kvFormat, RowType rowType) {
        return create(kvFormat, rowType.getChildren().toArray(new DataType[0]));
    }

    /**
     * Create a {@link RowEncoder} for encoding Java objects to a {@link MemorySegment} backed
     * {@link InternalRow}.
     */
    static RowEncoder create(KvFormat kvFormat, DataType[] fieldDataTypes) {
        if (kvFormat == KvFormat.COMPACTED) {
            return new CompactedRowEncoder(fieldDataTypes);
        } else if (kvFormat == KvFormat.INDEXED) {
            return new IndexedRowEncoder(fieldDataTypes);
        } else {
            throw new IllegalArgumentException("Unsupported kv format: " + kvFormat);
        }
    }

    /** Start to write a new row. */
    void startNewRow();

    /**
     * Write the row's field in given pos with given value.
     *
     * @param pos the pos of the field to write.
     * @param value the value of the field to write.
     */
    void encodeField(int pos, Object value);

    /**
     * Finish write the row, return the written row.
     *
     * @return the written row.
     */
    BinaryRow finishRow();
}
