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

import org.apache.flink.table.types.logical.LogicalType;

/**
 * An adapter for Variant type operations.
 *
 * <p>This default implementation throws {@link UnsupportedOperationException} for all operations.
 * Flink connector modules with native Variant support override this class with actual Variant
 * support.
 *
 * <p>TODO: remove this class when no longer support all the Flink 1.x series.
 *
 * @see org.apache.fluss.flink.adapter.SchemaAdapter for a similar adapter pattern
 */
public class VariantAdapter {
    private VariantAdapter() {}

    private static final String UNSUPPORTED_MSG =
            "Variant type is not supported by this Flink connector module. "
                    + "Please use a Flink connector version with native Variant support.";

    /** Whether the current Flink version supports the Variant type. */
    public static boolean supportVariant() {
        return false;
    }

    /** Extract metadata bytes from a Flink BinaryVariant object. */
    public static byte[] getMetadata(Object flinkVariant) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** Extract value bytes from a Flink BinaryVariant object. */
    public static byte[] getValue(Object flinkVariant) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** Get a Variant object from a Flink RowData at the given position. */
    public static Object getVariantFromRow(Object rowData, int pos) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** Get a Variant object from a Flink ArrayData at the given position. */
    public static Object getVariantFromArray(Object arrayData, int pos) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /**
     * Create a Flink BinaryVariant instance from metadata and value byte arrays.
     *
     * @param metadata the Variant metadata bytes
     * @param value the Variant value bytes
     * @return a Flink BinaryVariant object
     */
    public static Object createBinaryVariant(byte[] metadata, byte[] value) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /**
     * Create a Flink VariantType LogicalType instance.
     *
     * @param nullable whether the type is nullable
     * @return a Flink VariantType LogicalType
     */
    public static LogicalType flinkVariantType(boolean nullable) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
}
