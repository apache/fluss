/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.compression.ColumnValueCodec;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;

import java.util.BitSet;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Base row decoder that restores encoded column values after decoding the physical row. */
@Internal
public abstract class AbstractRowDecoder implements RowDecoder {

    private final KvFormat kvFormat;
    private final DataType[] fieldDataTypes;
    private final BitSet decodedColumns;
    private final ColumnValueCodec columnValueCodec;
    private final InternalRow.FieldGetter[] fieldGetters;

    protected AbstractRowDecoder(KvFormat kvFormat, DataType[] fieldDataTypes) {
        this(kvFormat, fieldDataTypes, new BitSet(), Collections.emptyMap());
    }

    protected AbstractRowDecoder(
            KvFormat kvFormat,
            DataType[] fieldDataTypes,
            BitSet decodedColumns,
            Map<Integer, String> tableConfigs) {
        this.kvFormat = checkNotNull(kvFormat, "kvFormat must not be null");
        this.fieldDataTypes = checkNotNull(fieldDataTypes, "fieldDataTypes must not be null");
        this.decodedColumns =
                (BitSet) checkNotNull(decodedColumns, "decodedColumns must not be null").clone();
        this.columnValueCodec = new ColumnValueCodec(tableConfigs);
        this.fieldGetters = new InternalRow.FieldGetter[fieldDataTypes.length];
        for (int i = 0; i < fieldDataTypes.length; i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(fieldDataTypes[i], i);
        }
    }

    /** Restores encoded fields and returns a row in the same physical format. */
    protected final BinaryRow decodeFields(BinaryRow row) {
        if (decodedColumns.isEmpty()) {
            return row;
        }

        Object[] values = new Object[fieldDataTypes.length];
        boolean changed = false;
        for (int i = 0; i < fieldDataTypes.length; i++) {
            Object value = fieldGetters[i].getFieldOrNull(row);
            if (value != null && decodedColumns.get(i)) {
                byte[] bytes = (byte[]) value;
                if (columnValueCodec.hasEnvelope(bytes)) {
                    value = columnValueCodec.decode(bytes);
                    changed = true;
                }
            }
            values[i] = value;
        }
        if (!changed) {
            return row;
        }

        RowEncoder encoder = RowEncoder.create(kvFormat, fieldDataTypes);
        encoder.startNewRow();
        for (int i = 0; i < values.length; i++) {
            encoder.encodeField(i, values[i]);
        }
        return encoder.finishRow();
    }
}
