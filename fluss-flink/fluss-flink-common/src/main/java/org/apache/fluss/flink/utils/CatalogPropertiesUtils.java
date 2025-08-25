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

package org.apache.fluss.flink.utils;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.fluss.utils.PropertiesUtils.excludeByPrefix;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Utilities for de/serializing {@link Catalog} objects into a map of string properties.
 *
 * <p>Copied from Flink.
 */
public class CatalogPropertiesUtils {

    // --------------------------------------------------------------------------------------------
    // Helper methods and constants
    // --------------------------------------------------------------------------------------------

    private static final String SEPARATOR = ".";

    private static final String SCHEMA = "schema";

    private static final String NAME = "name";

    private static final String DATA_TYPE = "data-type";

    private static final String EXPR = "expr";

    private static final String WATERMARK = "watermark";

    private static final String WATERMARK_ROWTIME = "rowtime";

    private static final String WATERMARK_STRATEGY = "strategy";

    private static final String WATERMARK_STRATEGY_EXPR = compoundKey(WATERMARK_STRATEGY, EXPR);

    private static final String WATERMARK_STRATEGY_DATA_TYPE =
            compoundKey(WATERMARK_STRATEGY, DATA_TYPE);

    private static final String COMMENT = "comment";
    private static final Pattern SCHEMA_COLUMN_NAME_SUFFIX = Pattern.compile("\\d+\\.name");

    public static Map<String, String> deserializeOptions(Map<String, String> map) {
        return excludeByPrefix(map, SCHEMA + SEPARATOR);
    }

    public static void deserializeWatermark(Map<String, String> map, Schema.Builder builder) {
        final String watermarkKey = compoundKey(SCHEMA, WATERMARK);
        final int watermarkCount = getCount(map, watermarkKey, WATERMARK_ROWTIME);
        for (int i = 0; i < watermarkCount; i++) {
            final String rowtimeKey = compoundKey(watermarkKey, i, WATERMARK_ROWTIME);
            final String exprKey = compoundKey(watermarkKey, i, WATERMARK_STRATEGY_EXPR);

            final String rowtime = getValue(map, rowtimeKey);
            final String expr = getValue(map, exprKey);
            builder.watermark(rowtime, expr);
        }
    }

    public static void deserializeComputedColumn(
            Map<String, String> map, int columnIndex, Schema.Builder builder) {
        final String nameKey = columnKey(columnIndex);
        final String exprKey = compoundKey(SCHEMA, columnIndex, EXPR);
        final String expr = getValue(map, exprKey);
        final String name = getValue(map, nameKey);
        builder.columnByExpression(name, expr);
    }

    public static void serializeComputedColumns(Map<String, String> map, List<Column> columns) {
        // field name -> index
        final List<Column> physicalCols = new ArrayList<>();
        final List<Integer> idxList = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i) instanceof Column.ComputedColumn) {
                physicalCols.add(columns.get(i));
                idxList.add(i);
            }
        }

        final String[] names = serializeColumnNames(physicalCols);
        final String[] dataTypes = serializeColumnDataTypes(physicalCols);
        final String[] expressions = serializeComputedColumns(physicalCols);
        final String[] comments = serializeColumnComments(physicalCols);
        final List<List<String>> values = new ArrayList<>();
        for (int i = 0; i < physicalCols.size(); i++) {
            values.add(Arrays.asList(names[i], dataTypes[i], expressions[i], comments[i]));
        }

        putIndexedProperties(
                map, SCHEMA, Arrays.asList(NAME, DATA_TYPE, EXPR, COMMENT), values, idxList);
    }

    public static int nonPhysicalColumnsCount(
            Map<String, String> tableOptions, List<String> physicalColumns) {
        int count = 0;
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (isColumnNameKey(entry.getKey()) && !physicalColumns.contains(entry.getValue())) {
                count++;
            }
        }
        return count;
    }

    private static boolean isColumnNameKey(String key) {
        return key.startsWith(SCHEMA)
                && SCHEMA_COLUMN_NAME_SUFFIX.matcher(key.substring(SCHEMA.length() + 1)).matches();
    }

    public static String[] serializeComputedColumns(List<Column> columns) {
        return columns.stream()
                .map(
                        column -> {
                            if (column instanceof Column.ComputedColumn) {
                                final Column.ComputedColumn c = (Column.ComputedColumn) column;
                                return serializeResolvedExpression(c.getExpression());
                            }
                            return null;
                        })
                .toArray(String[]::new);
    }

    private static String[] serializeColumnComments(List<Column> columns) {
        return columns.stream().map(c -> c.getComment().orElse(null)).toArray(String[]::new);
    }

    private static String[] serializeColumnNames(List<Column> columns) {
        return columns.stream().map(Column::getName).toArray(String[]::new);
    }

    private static String[] serializeColumnDataTypes(List<Column> columns) {
        return columns.stream()
                .map(Column::getDataType)
                .map(CatalogPropertiesUtils::serializeDataType)
                .toArray(String[]::new);
    }

    public static void serializeWatermarkSpecs(Map<String, String> map, List<WatermarkSpec> specs) {
        if (!specs.isEmpty()) {
            final List<List<String>> watermarkValues = new ArrayList<>();
            for (WatermarkSpec spec : specs) {
                watermarkValues.add(
                        Arrays.asList(
                                spec.getRowtimeAttribute(),
                                serializeResolvedExpression(spec.getWatermarkExpression()),
                                serializeDataType(
                                        spec.getWatermarkExpression().getOutputDataType())));
            }
            putIndexedProperties(
                    map,
                    compoundKey(SCHEMA, WATERMARK),
                    Arrays.asList(
                            WATERMARK_ROWTIME,
                            WATERMARK_STRATEGY_EXPR,
                            WATERMARK_STRATEGY_DATA_TYPE),
                    watermarkValues);
        }
    }

    /**
     * Adds an indexed sequence of properties (with sub-properties) under a common key. It supports
     * the property's value to be null, in which case it would be ignored. The sub-properties should
     * at least have one non-null value.
     *
     * <p>For example:
     *
     * <pre>
     *     schema.fields.0.type = INT, schema.fields.0.name = test
     *     schema.fields.1.type = LONG, schema.fields.1.name = test2
     *     schema.fields.2.type = LONG, schema.fields.2.name = test3, schema.fields.2.expr = test2 + 1
     * </pre>
     *
     * <p>The arity of each subKeyValues must match the arity of propertyKeys.
     */
    private static void putIndexedProperties(
            Map<String, String> map,
            String key,
            List<String> subKeys,
            List<List<String>> subKeyValues) {
        checkNotNull(key);
        checkNotNull(subKeys);
        checkNotNull(subKeyValues);
        for (int idx = 0; idx < subKeyValues.size(); idx++) {
            final List<String> values = subKeyValues.get(idx);
            if (values == null || values.size() != subKeys.size()) {
                throw new IllegalArgumentException("Values must have same arity as keys.");
            }
            if (values.stream().allMatch(Objects::isNull)) {
                throw new IllegalArgumentException("Values must have at least one non-null value.");
            }
            for (int keyIdx = 0; keyIdx < values.size(); keyIdx++) {
                String value = values.get(keyIdx);
                if (value != null) {
                    map.put(compoundKey(key, idx, subKeys.get(keyIdx)), values.get(keyIdx));
                }
            }
        }
    }

    /**
     * Adds an indexed sequence of properties (with sub-properties) under a common key with the
     * given idx. It supports the property's value to be null, in which case it would be ignored.
     * The sub-properties should at least have one non-null value.
     *
     * <p>For example:
     *
     * <pre>
     *     schema.fields.0.type = INT, schema.fields.0.name = test
     *     schema.fields.1.type = LONG, schema.fields.1.name = test2
     *     schema.fields.2.type = LONG, schema.fields.2.name = test3, schema.fields.2.expr = test2 + 1
     * </pre>
     *
     * <p>The arity of each subKeyValues must match the arity of propertyKeys.
     *
     * @param idxList the idx that each sub key will be
     */
    private static void putIndexedProperties(
            Map<String, String> map,
            String key,
            List<String> subKeys,
            List<List<String>> subKeyValues,
            List<Integer> idxList) {
        checkNotNull(key);
        checkNotNull(subKeys);
        checkNotNull(subKeyValues);
        for (int idx = 0; idx < subKeyValues.size(); idx++) {
            final List<String> values = subKeyValues.get(idx);
            if (values == null || values.size() != subKeys.size()) {
                throw new IllegalArgumentException("Values must have same arity as keys.");
            }
            if (values.stream().allMatch(Objects::isNull)) {
                throw new IllegalArgumentException("Values must have at least one non-null value.");
            }
            for (int keyIdx = 0; keyIdx < values.size(); keyIdx++) {
                String value = values.get(keyIdx);
                if (value != null) {
                    map.put(
                            compoundKey(key, idxList.get(idx), subKeys.get(keyIdx)),
                            values.get(keyIdx));
                }
            }
        }
    }

    public static String columnKey(int index) {
        return compoundKey(SCHEMA, index, NAME);
    }

    private static String compoundKey(Object... components) {
        return Stream.of(components).map(Object::toString).collect(Collectors.joining(SEPARATOR));
    }

    private static String serializeResolvedExpression(ResolvedExpression resolvedExpression) {
        try {
            return resolvedExpression.asSerializableString();
        } catch (TableException e) {
            throw new TableException(
                    String.format(
                            "Expression '%s' cannot be stored in a durable catalog. "
                                    + "Currently, only SQL expressions have a well-defined string "
                                    + "representation that is used to serialize a catalog object "
                                    + "into a map of string-based properties.",
                            resolvedExpression.asSummaryString()),
                    e);
        }
    }

    private static String serializeDataType(DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        try {
            return type.asSerializableString();
        } catch (TableException e) {
            throw new TableException(
                    String.format(
                            "Data type '%s' cannot be stored in a durable catalog. Only data types "
                                    + "that have a well-defined string representation can be used "
                                    + "when serializing a catalog object into a map of string-based "
                                    + "properties. This excludes anonymously defined, unregistered "
                                    + "types such as structured types in particular.",
                            type.asSummaryString()),
                    e);
        }
    }

    /**
     * Extracts the property count under the given key and suffix.
     *
     * <p>For example:
     *
     * <pre>
     *     schema.0.name, schema.1.name -> 2
     * </pre>
     */
    private static int getCount(Map<String, String> map, String key, String suffix) {
        final String escapedKey = Pattern.quote(key);
        final String escapedSuffix = Pattern.quote(suffix);
        final String escapedSeparator = Pattern.quote(SEPARATOR);
        final Pattern pattern =
                Pattern.compile(
                        "^"
                                + escapedKey
                                + escapedSeparator
                                + "(\\d+)"
                                + escapedSeparator
                                + escapedSuffix);
        final IntStream indexes =
                map.keySet().stream()
                        .flatMapToInt(
                                k -> {
                                    final Matcher matcher = pattern.matcher(k);
                                    if (matcher.find()) {
                                        return IntStream.of(Integer.parseInt(matcher.group(1)));
                                    }
                                    return IntStream.empty();
                                });

        return indexes.max().orElse(-1) + 1;
    }

    private static String getValue(Map<String, String> map, String key) {
        return getValue(map, key, Function.identity());
    }

    private static <T> T getValue(Map<String, String> map, String key, Function<String, T> parser) {
        final String value = map.get(key);
        if (value == null) {
            throw new IllegalArgumentException(
                    String.format("Could not find property key '%s'.", key));
        }
        try {
            return parser.apply(value);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Could not parse value for property key '%s': %s", key, value));
        }
    }

    private CatalogPropertiesUtils() {}
}
