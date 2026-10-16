/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable Kafka record mapping, shared by native DDL validation and Kafka request handling.
 * Resolving a contract never changes the schema or materializes configuration defaults.
 */
@Internal
public final class KafkaTableSchema {

    public static final String KEY_COLUMN = "_kafka_key";
    public static final String VALUE_COLUMN = "_kafka_value";
    public static final String HEADERS_COLUMN = "_kafka_headers";
    public static final String METADATA_COLUMN = "_kafka_metadata";
    public static final String RESCUE_COLUMN = "_kafka_rescue";

    /** Maximum recursive depth below each top-level JSON business field. */
    public static final int MAX_JSON_SCHEMA_NESTING_DEPTH = 64;

    private static final Set<String> RESERVED_NAMES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    KEY_COLUMN,
                                    VALUE_COLUMN,
                                    HEADERS_COLUMN,
                                    METADATA_COLUMN,
                                    RESCUE_COLUMN)));
    private static final Set<String> OPTIONS =
            new HashSet<>(
                    Arrays.asList(
                            ConfigOptions.TABLE_KAFKA_ENABLED.key(),
                            ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(),
                            ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(),
                            ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE.key()));

    /** Supported value conversion contracts; decoders are provided by the Kafka module. */
    public enum ValueType {
        BINARY,
        STRING,
        PARSE_JSON
    }

    /** JSON business fields either occupy the table row or the fixed value ROW column. */
    public enum ValueLayout {
        FLAT,
        NESTED
    }

    private final RowType rowType;
    private final ValueType valueType;
    private final @Nullable ValueLayout valueLayout;
    private final KafkaFieldProjection valueProjection;
    private final int keyPosition;
    private final int headersPosition;
    private final int metadataPosition;
    private final int rescuePosition;

    private KafkaTableSchema(
            RowType rowType,
            ValueType valueType,
            @Nullable ValueLayout valueLayout,
            List<Integer> valuePositions,
            int keyPosition,
            int headersPosition,
            int metadataPosition,
            int rescuePosition) {
        this.rowType = rowType;
        this.valueType = valueType;
        this.valueLayout = valueLayout;
        this.valueProjection = new KafkaFieldProjection(rowType, valuePositions);
        this.keyPosition = keyPosition;
        this.headersPosition = headersPosition;
        this.metadataPosition = metadataPosition;
        this.rescuePosition = rescuePosition;
    }

    /** Returns only the persisted opt-in flag; schema compatibility does not imply enablement. */
    public static boolean isEnabled(TableDescriptor table) {
        return new TableConfig(Configuration.fromMap(table.getProperties())).isKafkaEnabled();
    }

    /** Validates the Kafka contract only when the final table state enables it. */
    public static void validate(TableDescriptor table) {
        if (isEnabled(table)) {
            resolve(table);
        }
    }

    /** Checks whether the canonical topic name satisfies Fluss and Kafka name restrictions. */
    public static boolean isValidTablePath(TablePath path) {
        // Fluss identifiers are already restricted to Kafka's ASCII character set, without dots.
        return path != null
                && path.isValid()
                && TablePath.validatePrefix(path.getDatabaseName()) == null
                && TablePath.validatePrefix(path.getTableName()) == null
                && path.toString().length() <= 249;
    }

    /** Validates the canonical topic name before persisting an enabled table. */
    public static void validateTablePath(TablePath path, TableDescriptor table) {
        if (isEnabled(table) && !isValidTablePath(path)) {
            throw invalid("Table path cannot be represented as a Kafka topic: " + path);
        }
    }

    /** Returns whether a JSON member at the value mapping level has a reserved name. */
    public static boolean isReservedName(String name) {
        return RESERVED_NAMES.contains(name);
    }

    /** Resolves an explicitly enabled table or rejects its invalid contract. */
    public static KafkaTableSchema resolve(TableDescriptor table) {
        if (!isEnabled(table)) {
            throw invalid("Kafka compatibility is disabled; table.kafka.enabled must be true.");
        }
        validateOptions(table);
        if (table.hasPrimaryKey()
                || table.isPartitioned()
                || new TableConfig(Configuration.fromMap(table.getProperties())).getLogFormat()
                        != LogFormat.ARROW) {
            throw invalid(
                    "Kafka requires a non-partitioned Arrow log table without a primary key.");
        }
        Configuration options = Configuration.fromMap(table.getProperties());
        String value = options.get(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE);
        if (value == null) {
            throw invalid("Missing required table property 'table.kafka.convert.value.type'.");
        }
        ValueType valueType;
        try {
            valueType = ValueType.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw invalid(
                    "Unsupported table.kafka.convert.value.type '"
                            + value
                            + "'. Expected binary, string or parse_json.");
        }
        String layout = options.get(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT);
        ValueLayout valueLayout = null;
        if (valueType == ValueType.PARSE_JSON) {
            try {
                valueLayout =
                        layout == null
                                ? ValueLayout.FLAT
                                : ValueLayout.valueOf(layout.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw invalid("Unsupported table.kafka.convert.value.layout '" + layout + "'.");
            }
        } else if (layout != null) {
            throw invalid("table.kafka.convert.value.layout is only supported for parse_json.");
        }

        RowType rowType = table.getSchema().getRowType();
        int keyPosition = rowType.getFieldIndex(KEY_COLUMN);
        if (keyPosition >= 0 && !(rowType.getTypeAt(keyPosition) instanceof BytesType)) {
            throw invalid("_kafka_key must be BYTES.");
        }
        String keyType = options.get(ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE);
        if (keyType != null && (!"binary".equalsIgnoreCase(keyType.trim()) || keyPosition < 0)) {
            throw invalid(
                    "table.kafka.convert.key.type only supports binary and requires _kafka_key BYTES.");
        }
        int headersPosition = rowType.getFieldIndex(HEADERS_COLUMN);
        if (headersPosition >= 0) {
            DataType headers = rowType.getTypeAt(headersPosition);
            if (!(headers instanceof MapType)
                    || !(((MapType) headers).getKeyType() instanceof StringType)
                    || !(((MapType) headers).getValueType() instanceof BytesType)) {
                throw invalid("_kafka_headers must be MAP<STRING, BYTES>.");
            }
        }
        int rescuePosition = rowType.getFieldIndex(RESCUE_COLUMN);
        if (rescuePosition >= 0
                && (!(rowType.getTypeAt(rescuePosition) instanceof StringType)
                        || !rowType.getTypeAt(rescuePosition).isNullable())) {
            throw invalid("_kafka_rescue must be nullable STRING.");
        }
        int metadataPosition = rowType.getFieldIndex(METADATA_COLUMN);
        if (metadataPosition >= 0) {
            validateMetadataType(rowType.getTypeAt(metadataPosition));
        }

        List<Integer> valuePositions = new ArrayList<>();
        if (valueLayout == ValueLayout.FLAT) {
            if (rowType.getFieldIndex(VALUE_COLUMN) >= 0) {
                throw invalid(
                        "_kafka_value is reserved for binary, string or nested JSON; "
                                + "it cannot be a flat JSON business field.");
            }
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (!isReservedName(rowType.getFieldNames().get(i))) {
                    valuePositions.add(i);
                }
            }
        } else {
            int valuePosition = rowType.getFieldIndex(VALUE_COLUMN);
            if (valuePosition < 0) {
                throw invalid("Missing required value column _kafka_value.");
            }
            DataType type = rowType.getTypeAt(valuePosition);
            boolean valid =
                    valueType == ValueType.BINARY
                            ? type instanceof BytesType
                            : valueType == ValueType.STRING
                                    ? type instanceof StringType
                                    : type instanceof RowType;
            if (!valid) {
                throw invalid(
                        "_kafka_value must be "
                                + (valueType == ValueType.BINARY
                                        ? "BYTES"
                                        : valueType == ValueType.STRING ? "STRING" : "ROW")
                                + " for "
                                + valueType
                                + ".");
            }
            for (String name : rowType.getFieldNames()) {
                if (!isReservedName(name)) {
                    throw invalid(
                            "Column '"
                                    + name
                                    + "' has no value mapping; this conversion targets _kafka_value only.");
                }
            }
            if (type instanceof RowType) {
                for (String name : ((RowType) type).getFieldNames()) {
                    if (isReservedName(name)) {
                        throw invalid(
                                "Reserved name '"
                                        + name
                                        + "' cannot be a nested JSON business field.");
                    }
                }
            }
            valuePositions.add(valuePosition);
        }
        if (valueType == ValueType.PARSE_JSON) {
            for (int position : valuePositions) {
                DataType type = rowType.getTypeAt(position);
                if (valueLayout == ValueLayout.NESTED) {
                    // The physical value container represents the JSON object root, just like
                    // the table row in flat layout; it must not add a business nesting level.
                    for (DataType field : ((RowType) type).getChildren()) {
                        validateJsonType(field);
                    }
                } else {
                    validateJsonType(type);
                }
            }
        }
        return new KafkaTableSchema(
                rowType,
                valueType,
                valueLayout,
                valuePositions,
                keyPosition,
                headersPosition,
                metadataPosition,
                rescuePosition);
    }

    /**
     * Validates a top-level JSON business field without parser or protocol dependencies. DDL and
     * decoder construction share this contract. Each business field starts at depth zero; ROW
     * fields, ARRAY elements and MAP values each advance the depth by one. Nullability and scalar
     * value constraints are enforced when decoding individual records. The JSON object root
     * (including the physical nested _kafka_value ROW) is not counted.
     */
    public static void validateJsonType(DataType type) {
        validateJsonType(type, 0);
    }

    private static void validateJsonType(DataType type, int depth) {
        if (depth > MAX_JSON_SCHEMA_NESTING_DEPTH) {
            throw invalid(
                    "Kafka JSON schema exceeds the maximum nesting depth of "
                            + MAX_JSON_SCHEMA_NESTING_DEPTH
                            + ".");
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
            case BOOLEAN:
            case BINARY:
            case BYTES:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return;
            case ARRAY:
                validateJsonType(((ArrayType) type).getElementType(), depth + 1);
                return;
            case MAP:
                MapType map = (MapType) type;
                if (!(map.getKeyType() instanceof StringType)) {
                    throw invalid(
                            "Kafka JSON format only supports STRING map keys, but found "
                                    + type.asSummaryString()
                                    + ".");
                }
                validateJsonType(map.getValueType(), depth + 1);
                return;
            case ROW:
                for (DataType field : ((RowType) type).getChildren()) {
                    validateJsonType(field, depth + 1);
                }
                return;
            default:
                throw invalid(
                        "Kafka JSON format does not support Fluss type "
                                + type.asSummaryString()
                                + ".");
        }
    }

    private static void validateOptions(TableDescriptor table) {
        for (String name : table.getProperties().keySet()) {
            if (name.startsWith("table.kafka.") && !OPTIONS.contains(name)) {
                throw invalid("Unsupported Kafka table property '" + name + "'.");
            }
        }
        for (String name : table.getCustomProperties().keySet()) {
            if (name.startsWith("table.kafka.")
                    || name.startsWith("kafka.")
                    || name.startsWith("fluss.")) {
                throw invalid(
                        "Unsupported Kafka custom property '"
                                + name
                                + "'. Use registered table.kafka.* properties and fixed system columns.");
            }
        }
    }

    private static void validateMetadataType(DataType type) {
        String message =
                "_kafka_metadata must be ROW<timestamp TIMESTAMP_LTZ(3), "
                        + "timestamp_type INT, partition INT>.";
        if (!(type instanceof RowType)) {
            throw invalid(message);
        }
        RowType row = (RowType) type;
        if (!row.getFieldNames().equals(Arrays.asList("timestamp", "timestamp_type", "partition"))
                || !row.getTypeAt(0).copy(true).equals(DataTypes.TIMESTAMP_LTZ(3))
                || !row.getTypeAt(1).copy(true).equals(DataTypes.INT())
                || !row.getTypeAt(2).copy(true).equals(DataTypes.INT())) {
            throw invalid(message);
        }
    }

    /** Returns the unchanged physical schema, including declared nullability. */
    public RowType rowType() {
        return rowType;
    }

    /** Returns the value conversion type. */
    public ValueType valueType() {
        return valueType;
    }

    /** Returns the JSON layout, or null for binary/string. */
    public @Nullable ValueLayout valueLayout() {
        return valueLayout;
    }

    /** Returns physical value targets; nested JSON projects its single ROW container. */
    public KafkaFieldProjection valueProjection() {
        return valueProjection;
    }

    /** Returns the key column position, or -1 when keys are not stored. */
    public int keyPosition() {
        return keyPosition;
    }

    /**
     * Returns the headers column position, or -1 when headers are not stored. Decoding maps names
     * to binary values, with the last occurrence winning for duplicate header names.
     */
    public int headersPosition() {
        return headersPosition;
    }

    /**
     * Returns the metadata column position, or -1 when absent. The ROW stores the input record
     * timestamp, batch timestamp type and requested partition; log offsets are not row fields.
     */
    public int metadataPosition() {
        return metadataPosition;
    }

    /**
     * Returns the rescue column position, or -1 when absent. JSON decoders collect unknown and
     * reserved members here; binary/string decoders write null to this optional column.
     */
    public int rescuePosition() {
        return rescuePosition;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof KafkaTableSchema)) {
            return false;
        }
        KafkaTableSchema that = (KafkaTableSchema) other;
        return rowType.equals(that.rowType)
                && valueType == that.valueType
                && valueLayout == that.valueLayout
                && valueProjection.equals(that.valueProjection)
                && keyPosition == that.keyPosition
                && headersPosition == that.headersPosition
                && metadataPosition == that.metadataPosition
                && rescuePosition == that.rescuePosition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rowType,
                valueType,
                valueLayout,
                valueProjection,
                keyPosition,
                headersPosition,
                metadataPosition,
                rescuePosition);
    }

    private static InvalidTableException invalid(String message) {
        return new InvalidTableException(message);
    }
}
