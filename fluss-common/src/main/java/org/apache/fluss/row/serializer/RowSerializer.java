/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.serializer;

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.AlignedRowEncoder;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.IndexedRowEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.DataType;

import java.io.Serializable;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.ALIGNED;
import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.COMPACTED;
import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;

/** Serializer for {@link InternalRow} to {@link BinaryRow}. */
public class RowSerializer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final DataType[] fieldTypes;
    private final BinaryRowFormat format;

    private transient BinaryRowSerializer serializer;

    public RowSerializer(DataType[] fieldTypes, BinaryRowFormat format) {
        this.fieldTypes = fieldTypes;
        this.format = format;
    }

    /**
     * Serialize the given {@link InternalRow} into {@link BinaryRow}.
     *
     * <p>The returned {@link BinaryRow} might reuse the memory from the input {@link InternalRow}
     * if it is already a {@link BinaryRow}.
     *
     * <p>Otherwise, it will serialize a {@link BinaryRow} based on the specified {@link
     * BinaryRowFormat}.
     */
    public BinaryRow toBinaryRow(InternalRow from) {
        if (from instanceof BinaryRow) {
            if (format == INDEXED && from instanceof IndexedRow
                    || format == COMPACTED && from instanceof CompactedRow
                    || format == ALIGNED && from instanceof AlignedRow) {
                // directly return the original row iff the row is in the expected format
                return (BinaryRow) from;
            }
        }

        if (serializer == null) {
            serializer = new BinaryRowSerializer(fieldTypes, format);
        }
        return serializer.toBinaryRow(from);
    }

    /**
     * Serializer function for BinaryRow, it delegates the actual encoding to different {@link
     * RowEncoder} based on the specified format.
     */
    private static class BinaryRowSerializer {
        private final RowEncoder rowEncoder;
        private final InternalRow.FieldGetter[] fieldGetters;

        private BinaryRowSerializer(DataType[] fieldTypes, BinaryRowFormat format) {
            this.fieldGetters = new InternalRow.FieldGetter[fieldTypes.length];
            for (int i = 0; i < fieldTypes.length; i++) {
                this.fieldGetters[i] = InternalRow.createFieldGetter(fieldTypes[i], i);
            }
            switch (format) {
                case COMPACTED:
                    this.rowEncoder = new CompactedRowEncoder(fieldTypes);
                    break;
                case INDEXED:
                    this.rowEncoder = new IndexedRowEncoder(fieldTypes);
                    break;
                case ALIGNED:
                    this.rowEncoder = new AlignedRowEncoder(fieldTypes);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported binary row format: " + format);
            }
        }

        public BinaryRow toBinaryRow(InternalRow from) {
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldGetters.length; i++) {
                rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(from));
            }
            return rowEncoder.finishRow();
        }
    }
}
