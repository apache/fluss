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

package org.apache.fluss.lake.delta.utils;

import org.apache.fluss.lake.delta.utils.FlussRowToDeltaValues.MapEntries;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A {@link ColumnarBatch} over rows copied by {@link FlussRowToDeltaValues}.
 *
 * <p>Delta Kernel reads data column by column, while Fluss hands records over row by row. Each
 * column of this batch is a view over one field of the buffered rows, so no data is copied again.
 */
public class DeltaColumnarBatch implements ColumnarBatch {

    private final StructType schema;
    private final List<ColumnVector> columns;
    private final int size;

    private DeltaColumnarBatch(StructType schema, List<ColumnVector> columns, int size) {
        checkArgument(
                schema.length() == columns.size(),
                "Schema has %s fields but there are %s columns.",
                schema.length(),
                columns.size());
        this.schema = schema;
        this.columns = columns;
        this.size = size;
    }

    /** Creates a batch whose i-th column reads the i-th value of every row. */
    public static DeltaColumnarBatch of(StructType schema, List<Object[]> rows) {
        List<ColumnVector> columns = new ArrayList<>(schema.length());
        for (int i = 0; i < schema.length(); i++) {
            columns.add(new ObjectColumnVector(schema.at(i).getDataType(), fieldValues(rows, i)));
        }
        return new DeltaColumnarBatch(schema, columns, rows.size());
    }

    @Override
    public StructType getSchema() {
        return schema;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal) {
        return columns.get(ordinal);
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public ColumnarBatch withNewColumn(
            int ordinal, StructField columnSchema, ColumnVector columnVector) {
        List<StructField> fields = new ArrayList<>(schema.fields());
        fields.add(ordinal, columnSchema);
        List<ColumnVector> newColumns = new ArrayList<>(columns);
        newColumns.add(ordinal, columnVector);
        return new DeltaColumnarBatch(new StructType(fields), newColumns, size);
    }

    @Override
    public ColumnarBatch withDeletedColumnAt(int ordinal) {
        List<StructField> fields = new ArrayList<>(schema.fields());
        fields.remove(ordinal);
        List<ColumnVector> newColumns = new ArrayList<>(columns);
        newColumns.remove(ordinal);
        return new DeltaColumnarBatch(new StructType(fields), newColumns, size);
    }

    @Override
    public ColumnarBatch withNewSchema(StructType newSchema) {
        return new DeltaColumnarBatch(newSchema, columns, size);
    }

    /** A view over the {@code ordinal}-th value of each row, a null row gives a null value. */
    private static List<Object> fieldValues(List<?> rows, int ordinal) {
        return new AbstractList<Object>() {
            @Override
            public Object get(int index) {
                Object[] row = (Object[]) rows.get(index);
                return row == null ? null : row[ordinal];
            }

            @Override
            public int size() {
                return rows.size();
            }
        };
    }

    /** A {@link ColumnVector} over a list of values, {@code null} stands for a null value. */
    static class ObjectColumnVector implements ColumnVector {

        private final DataType dataType;
        private final List<?> values;

        ObjectColumnVector(DataType dataType, List<?> values) {
            this.dataType = dataType;
            this.values = values;
        }

        @Override
        public DataType getDataType() {
            return dataType;
        }

        @Override
        public int getSize() {
            return values.size();
        }

        @Override
        public void close() {}

        @Override
        public boolean isNullAt(int rowId) {
            return values.get(rowId) == null;
        }

        @Override
        public boolean getBoolean(int rowId) {
            return (Boolean) values.get(rowId);
        }

        @Override
        public byte getByte(int rowId) {
            return (Byte) values.get(rowId);
        }

        @Override
        public short getShort(int rowId) {
            return (Short) values.get(rowId);
        }

        @Override
        public int getInt(int rowId) {
            return (Integer) values.get(rowId);
        }

        @Override
        public long getLong(int rowId) {
            return (Long) values.get(rowId);
        }

        @Override
        public float getFloat(int rowId) {
            return (Float) values.get(rowId);
        }

        @Override
        public double getDouble(int rowId) {
            return (Double) values.get(rowId);
        }

        @Override
        public byte[] getBinary(int rowId) {
            return (byte[]) values.get(rowId);
        }

        @Override
        public String getString(int rowId) {
            return (String) values.get(rowId);
        }

        @Override
        public BigDecimal getDecimal(int rowId) {
            return (BigDecimal) values.get(rowId);
        }

        @Override
        public ArrayValue getArray(int rowId) {
            List<?> elements = (List<?>) values.get(rowId);
            if (elements == null) {
                return null;
            }
            ColumnVector elementVector =
                    new ObjectColumnVector(((ArrayType) dataType).getElementType(), elements);
            return new ArrayValue() {
                @Override
                public int getSize() {
                    return elements.size();
                }

                @Override
                public ColumnVector getElements() {
                    return elementVector;
                }
            };
        }

        @Override
        public MapValue getMap(int rowId) {
            MapEntries entries = (MapEntries) values.get(rowId);
            if (entries == null) {
                return null;
            }
            MapType mapType = (MapType) dataType;
            ColumnVector keyVector = new ObjectColumnVector(mapType.getKeyType(), entries.keys());
            ColumnVector valueVector =
                    new ObjectColumnVector(mapType.getValueType(), entries.values());
            return new MapValue() {
                @Override
                public int getSize() {
                    return entries.keys().size();
                }

                @Override
                public ColumnVector getKeys() {
                    return keyVector;
                }

                @Override
                public ColumnVector getValues() {
                    return valueVector;
                }
            };
        }

        @Override
        public ColumnVector getChild(int ordinal) {
            DataType childType = ((StructType) dataType).at(ordinal).getDataType();
            return new ObjectColumnVector(childType, fieldValues(values, ordinal));
        }
    }
}
