/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.serializer;

import org.apache.fluss.memory.InputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryWriter;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowSerializer;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowSerializer;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

/** InternalRowSerializer. */
public class InternalRowSerializer implements Serializer<InternalRow> {

    private final DataType[] types;
    private final Serializer[] fieldSerializers;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final BinaryWriter.ValueSetter[] valueSetters;
    private final KvFormat kvFormat;

    private transient BinaryRow reuseRow;
    private transient BinaryWriter reuseWriter;

    public InternalRowSerializer(RowType rowType) {
        this(rowType.getFieldTypes().toArray(new DataType[0]), KvFormat.INDEXED);
    }

    public InternalRowSerializer(RowType rowType, KvFormat kvFormat) {
        this(rowType.getFieldTypes().toArray(new DataType[0]), kvFormat);
    }

    public InternalRowSerializer(DataType[] types) {
        this(
                types,
                Arrays.stream(types).map(InternalSerializers::create).toArray(Serializer[]::new),
                KvFormat.INDEXED);
    }

    public InternalRowSerializer(DataType[] types, KvFormat kvFormat) {
        this(
                types,
                Arrays.stream(types)
                        .map(createSerializerFunction(kvFormat))
                        .toArray(Serializer[]::new),
                kvFormat);
    }

    public InternalRowSerializer(
            DataType[] types, Serializer<?>[] fieldSerializers, KvFormat kvFormat) {
        this.types = types;
        this.fieldSerializers = fieldSerializers;
        this.fieldGetters = new InternalRow.FieldGetter[types.length];
        this.valueSetters = new BinaryWriter.ValueSetter[types.length];
        for (int i = 0; i < types.length; i++) {
            DataType type = types[i];
            fieldGetters[i] = InternalRow.createFieldGetter(type, i);
            // pass serializer to avoid infinite loop
            valueSetters[i] = BinaryWriter.createValueSetter(type, fieldSerializers[i], kvFormat);
        }
        this.kvFormat = kvFormat;
    }

    @Override
    public InternalRowSerializer duplicate() {
        Serializer<?>[] duplicateFieldSerializers = new Serializer[fieldSerializers.length];
        for (int i = 0; i < fieldSerializers.length; i++) {
            duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
        }
        return new InternalRowSerializer(types, duplicateFieldSerializers, kvFormat);
    }

    @Override
    public InternalRow copy(InternalRow from) {
        if (from.getFieldCount() != types.length) {
            throw new IllegalArgumentException(
                    "Row arity: "
                            + from.getFieldCount()
                            + ", but serializer arity: "
                            + types.length);
        }
        if (from instanceof BinaryRow) {
            return ((BinaryRow) from).copy();
        } else {
            return copyRowData(from, new GenericRow(from.getFieldCount()));
        }
    }

    @Override
    public void serialize(InternalRow row, OutputView target) throws IOException {
        BinaryRow binaryRow = toBinaryRow(row);
        target.writeInt(binaryRow.getSizeInBytes());
        BinarySegmentUtils.copyToView(
                binaryRow.getSegments(), binaryRow.getOffset(), binaryRow.getSizeInBytes(), target);
    }

    @Override
    public InternalRow deserialize(InputView source) throws IOException {
        BinaryRow binaryRow = getBinaryRow();

        return deserializeReuse(binaryRow, source);
    }

    private BinaryRow getBinaryRow() {
        BinaryRow binaryRow;
        if (kvFormat == KvFormat.INDEXED) {
            binaryRow = new IndexedRow(types);

        } else if (kvFormat == KvFormat.COMPACTED) {
            binaryRow = new CompactedRow(types);
        } else {
            throw new IllegalArgumentException("Unsupported kv format: " + kvFormat);
        }
        return binaryRow;
    }

    private BinaryRow deserializeReuse(BinaryRow reuse, InputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
        return reuse;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof InternalRowSerializer) {
            InternalRowSerializer other = (InternalRowSerializer) obj;
            return Arrays.equals(fieldSerializers, other.fieldSerializers);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fieldSerializers);
    }

    @SuppressWarnings("unchecked")
    public InternalRow copyRowData(InternalRow from, InternalRow reuse) {
        GenericRow ret;
        if (reuse instanceof GenericRow) {
            ret = (GenericRow) reuse;
        } else {
            ret = new GenericRow(from.getFieldCount());
        }

        for (int i = 0; i < from.getFieldCount(); i++) {
            if (!from.isNullAt(i)) {
                ret.setField(i, fieldSerializers[i].copy((fieldGetters[i].getFieldOrNull(from))));
            } else {
                ret.setField(i, null);
            }
        }
        return ret;
    }

    public BinaryRow toBinaryRow(InternalRow row) {
        if (kvFormat == KvFormat.INDEXED) {
            reuseRow = new IndexedRow(types);
            reuseWriter = new IndexedRowWriter(types);
        } else if (kvFormat == KvFormat.COMPACTED) {
            reuseRow = new CompactedRow(types);
            reuseWriter = new CompactedRowWriter(types);
        } else {
            throw new IllegalArgumentException("Unsupported kv format: " + kvFormat);
        }

        reuseWriter.reset();

        for (int i = 0; i < types.length; i++) {
            Object field = fieldGetters[i].getFieldOrNull(row);
            if (field == null) {
                reuseWriter.setNullAt(i);
            } else {
                valueSetters[i].setValue(reuseWriter, i, field);
            }
        }

        reuseWriter.complete();

        reuseRow.pointTo(reuseWriter.segment(), 0, reuseWriter.position());

        return reuseRow;
    }

    public int getArity() {
        return types.length;
    }

    public KvFormat getKvFormat() {
        return kvFormat;
    }

    private static Function<DataType, Serializer<Object>> createSerializerFunction(
            KvFormat kvFormat) {
        return type -> {
            if (kvFormat == KvFormat.COMPACTED) {
                return CompactedRowSerializer.create(type);
            } else {
                return IndexedRowSerializer.create(type);
            }
        };
    }
}
