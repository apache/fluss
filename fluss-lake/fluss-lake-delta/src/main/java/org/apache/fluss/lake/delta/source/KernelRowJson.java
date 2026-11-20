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

package org.apache.fluss.lake.delta.source;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Optional;

/**
 * Writes the Delta Kernel rows of a scan, the scan state and the scan files, as JSON so that they
 * can travel inside a split, and parses them back with the {@code JsonHandler} of the engine.
 *
 * <p>The rows only hold strings, integers, longs, booleans, structs, string-keyed maps and arrays
 * of these, which is what this writer supports. Null fields are left out, as the JSON handler reads
 * a missing field as null.
 */
final class KernelRowJson {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private KernelRowJson() {}

    static String toJson(Row row) throws IOException {
        StringWriter writer = new StringWriter();
        try (JsonGenerator generator = JSON_FACTORY.createGenerator(writer)) {
            writeStruct(generator, row);
        }
        return writer.toString();
    }

    static Row fromJson(Engine engine, String json, StructType schema) throws IOException {
        ColumnarBatch batch =
                engine.getJsonHandler()
                        .parseJson(new SingleStringVector(json), schema, Optional.empty());
        try (CloseableIterator<Row> rows = batch.getRows()) {
            return rows.next();
        }
    }

    private static void writeStruct(JsonGenerator generator, Row row) throws IOException {
        StructType schema = row.getSchema();
        generator.writeStartObject();
        for (int i = 0; i < schema.length(); i++) {
            if (row.isNullAt(i)) {
                continue;
            }
            DataType type = schema.at(i).getDataType();
            generator.writeFieldName(schema.at(i).getName());
            if (type instanceof StringType) {
                generator.writeString(row.getString(i));
            } else if (type instanceof LongType) {
                generator.writeNumber(row.getLong(i));
            } else if (type instanceof IntegerType) {
                generator.writeNumber(row.getInt(i));
            } else if (type instanceof BooleanType) {
                generator.writeBoolean(row.getBoolean(i));
            } else if (type instanceof StructType) {
                writeStruct(generator, row.getStruct(i));
            } else if (type instanceof MapType) {
                writeMap(generator, row.getMap(i));
            } else if (type instanceof ArrayType) {
                writeArray(generator, row.getArray(i));
            } else {
                throw unsupported(type);
            }
        }
        generator.writeEndObject();
    }

    private static void writeMap(JsonGenerator generator, MapValue map) throws IOException {
        if (!(map.getKeys().getDataType() instanceof StringType)) {
            throw unsupported(map.getKeys().getDataType());
        }
        generator.writeStartObject();
        for (int i = 0; i < map.getSize(); i++) {
            generator.writeFieldName(map.getKeys().getString(i));
            writeElement(generator, map.getValues(), i);
        }
        generator.writeEndObject();
    }

    private static void writeArray(JsonGenerator generator, ArrayValue array) throws IOException {
        generator.writeStartArray();
        for (int i = 0; i < array.getSize(); i++) {
            writeElement(generator, array.getElements(), i);
        }
        generator.writeEndArray();
    }

    private static void writeElement(JsonGenerator generator, ColumnVector vector, int rowId)
            throws IOException {
        if (vector.isNullAt(rowId)) {
            generator.writeNull();
            return;
        }
        DataType type = vector.getDataType();
        if (type instanceof StringType) {
            generator.writeString(vector.getString(rowId));
        } else if (type instanceof LongType) {
            generator.writeNumber(vector.getLong(rowId));
        } else if (type instanceof IntegerType) {
            generator.writeNumber(vector.getInt(rowId));
        } else if (type instanceof BooleanType) {
            generator.writeBoolean(vector.getBoolean(rowId));
        } else {
            throw unsupported(type);
        }
    }

    private static UnsupportedOperationException unsupported(DataType type) {
        return new UnsupportedOperationException(
                "Can't write a Delta Kernel scan row with a value of type " + type + " as JSON.");
    }

    /** The single JSON document handed to {@code JsonHandler#parseJson}. */
    private static final class SingleStringVector implements ColumnVector {

        private final String value;

        private SingleStringVector(String value) {
            this.value = value;
        }

        @Override
        public DataType getDataType() {
            return StringType.STRING;
        }

        @Override
        public int getSize() {
            return 1;
        }

        @Override
        public void close() {}

        @Override
        public boolean isNullAt(int rowId) {
            return false;
        }

        @Override
        public String getString(int rowId) {
            return value;
        }
    }
}
