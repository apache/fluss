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

package org.apache.fluss.flink.adapter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.types.variant.BinaryVariant;

/**
 * Flink 2.2 implementation of the Variant adapter.
 *
 * <p>This class provides actual Variant type support using Flink 2.2's native Variant APIs ({@link
 * BinaryVariant}, {@link VariantType}).
 *
 * @see org.apache.fluss.flink.adapter.SchemaAdapter for a similar adapter pattern
 */
public class VariantAdapter {
    private VariantAdapter() {}

    /** Whether the current Flink version supports the Variant type. */
    public static boolean supportVariant() {
        return true;
    }

    /** Extract metadata bytes from a Flink BinaryVariant object. */
    public static byte[] getMetadata(Object flinkVariant) {
        return ((BinaryVariant) flinkVariant).getMetadata();
    }

    /** Extract value bytes from a Flink BinaryVariant object. */
    public static byte[] getValue(Object flinkVariant) {
        return ((BinaryVariant) flinkVariant).getValue();
    }

    /** Get a Variant object from a Flink RowData at the given position. */
    public static Object getVariantFromRow(Object rowData, int pos) {
        return ((RowData) rowData).getVariant(pos);
    }

    /** Get a Variant object from a Flink ArrayData at the given position. */
    public static Object getVariantFromArray(Object arrayData, int pos) {
        return ((ArrayData) arrayData).getVariant(pos);
    }

    /**
     * Create a Flink BinaryVariant instance from metadata and value byte arrays.
     *
     * @param metadata the Variant metadata bytes
     * @param value the Variant value bytes
     * @return a Flink BinaryVariant object
     */
    public static Object createBinaryVariant(byte[] metadata, byte[] value) {
        return new BinaryVariant(metadata, value);
    }

    /**
     * Create a Flink VariantType LogicalType instance.
     *
     * @param nullable whether the type is nullable
     * @return a Flink VariantType LogicalType
     */
    public static LogicalType flinkVariantType(boolean nullable) {
        return new VariantType(nullable);
    }
}
