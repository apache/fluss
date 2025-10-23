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

package org.apache.fluss.client.converter;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.client.converter.RowToPojoConverter.charLengthExceptionMessage;

/**
 * Internal shared utilities for POJO and Fluss InternalRow conversions.
 *
 * <p>Provides validation helpers and common functions used by PojoToRowConverter and
 * RowToPojoConverter (e.g., supported Java types per Fluss DataType, projection/table validation,
 * and text conversion helpers).
 */
final class ConverterCommons {

    static final Map<DataTypeRoot, Set<Class<?>>> SUPPORTED_TYPES = createSupportedTypes();

    private static Map<DataTypeRoot, Set<Class<?>>> createSupportedTypes() {
        Map<DataTypeRoot, Set<Class<?>>> map = new HashMap<>();
        map.put(DataTypeRoot.BOOLEAN, setOf(Boolean.class));
        map.put(DataTypeRoot.TINYINT, setOf(Byte.class));
        map.put(DataTypeRoot.SMALLINT, setOf(Short.class));
        map.put(DataTypeRoot.INTEGER, setOf(Integer.class));
        map.put(DataTypeRoot.BIGINT, setOf(Long.class));
        map.put(DataTypeRoot.FLOAT, setOf(Float.class));
        map.put(DataTypeRoot.DOUBLE, setOf(Double.class));
        map.put(DataTypeRoot.CHAR, setOf(String.class, Character.class));
        map.put(DataTypeRoot.STRING, setOf(String.class, Character.class));
        map.put(DataTypeRoot.BINARY, setOf(byte[].class));
        map.put(DataTypeRoot.BYTES, setOf(byte[].class));
        map.put(DataTypeRoot.DECIMAL, setOf(BigDecimal.class));
        map.put(DataTypeRoot.DATE, setOf(java.time.LocalDate.class));
        map.put(DataTypeRoot.TIME_WITHOUT_TIME_ZONE, setOf(java.time.LocalTime.class));
        map.put(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, setOf(java.time.LocalDateTime.class));
        map.put(
                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                setOf(java.time.Instant.class, java.time.OffsetDateTime.class));
        return map;
    }

    static void validatePojoMatchesTable(PojoType<?> pojoType, RowType tableSchema) {
        Set<String> pojoNames = pojoType.getProperties().keySet();
        Set<String> tableNames = new HashSet<>(tableSchema.getFieldNames());
        if (!pojoNames.equals(tableNames)) {
            throw new IllegalArgumentException(
                    String.format(
                            "POJO fields %s must exactly match table schema fields %s.",
                            pojoNames, tableNames));
        }
        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            String name = tableSchema.getFieldNames().get(i);
            DataType dt = tableSchema.getTypeAt(i);
            PojoType.Property prop = pojoType.getProperty(name);
            validateCompatibility(dt, prop);
        }
    }

    static void validateProjectionSubset(RowType projection, RowType tableSchema) {
        Set<String> tableNames = new HashSet<>(tableSchema.getFieldNames());
        for (String n : projection.getFieldNames()) {
            if (!tableNames.contains(n)) {
                throw new IllegalArgumentException(
                        "Projection field '" + n + "' is not part of table schema.");
            }
        }
    }

    static void validateCompatibility(DataType fieldType, PojoType.Property prop) {
        Set<Class<?>> supported = SUPPORTED_TYPES.get(fieldType.getTypeRoot());
        Class<?> actual = prop.type;
        if (supported == null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported field type %s for field %s.",
                            fieldType.getTypeRoot(), prop.name));
        }
        if (!supported.contains(actual)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field '%s' in POJO has Java type %s which is incompatible with Fluss type %s. Supported Java types: %s",
                            prop.name, actual.getName(), fieldType.getTypeRoot(), supported));
        }
    }

    static BinaryString toBinaryStringForText(Object v, String fieldName, DataTypeRoot root) {
        final String s = String.valueOf(v);
        if (root == DataTypeRoot.CHAR && s.length() != 1) {
            throw new IllegalArgumentException(charLengthExceptionMessage(fieldName, s.length()));
        }
        return BinaryString.fromString(s);
    }

    static Set<Class<?>> setOf(Class<?>... javaTypes) {
        return new HashSet<>(Arrays.asList(javaTypes));
    }
}
