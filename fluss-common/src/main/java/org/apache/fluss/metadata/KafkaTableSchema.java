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
import org.apache.fluss.config.KafkaValueConversionLayout;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
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
import java.util.Objects;
import java.util.Set;

import static org.apache.fluss.types.DataTypeChecks.equalsIgnoreFieldDescriptionsAndIds;

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

    private static final DataType KEY_TYPE = DataTypes.BYTES();
    private static final DataType HEADERS_TYPE =
            DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES());
    private static final RowType METADATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                    DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                    DataTypes.FIELD("partition", DataTypes.INT()));
    private static final DataType RESCUE_TYPE = DataTypes.STRING();
    private static final DataType BINARY_VALUE_TYPE = DataTypes.BYTES();
    private static final DataType STRING_VALUE_TYPE = DataTypes.STRING();

    private static final Set<String> RESERVED_NAMES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    KEY_COLUMN,
                                    VALUE_COLUMN,
                                    HEADERS_COLUMN,
                                    METADATA_COLUMN,
                                    RESCUE_COLUMN)));

    private final RowType rowType;
    private final KafkaValueConversionType valueConversionType;
    private final @Nullable KafkaValueConversionLayout valueConversionLayout;
    private final KafkaFieldProjection valueProjection;
    private final int keyPosition;
    private final int headersPosition;
    private final int metadataPosition;
    private final int rescuePosition;

    private KafkaTableSchema(
            RowType rowType,
            KafkaValueConversionType valueConversionType,
            @Nullable KafkaValueConversionLayout valueConversionLayout,
            List<Integer> valuePositions,
            int keyPosition,
            int headersPosition,
            int metadataPosition,
            int rescuePosition) {
        this.rowType = rowType;
        this.valueConversionType = valueConversionType;
        this.valueConversionLayout = valueConversionLayout;
        this.valueProjection = new KafkaFieldProjection(rowType, valuePositions);
        this.keyPosition = keyPosition;
        this.headersPosition = headersPosition;
        this.metadataPosition = metadataPosition;
        this.rescuePosition = rescuePosition;
    }

    /** Returns only the persisted opt-in flag; schema compatibility does not imply enablement. */
    public static boolean isEnabled(TableDescriptor table) {
        String value = table.getProperties().get(ConfigOptions.TABLE_KAFKA_ENABLED.key());
        if (value == null) {
            return ConfigOptions.TABLE_KAFKA_ENABLED.defaultValue();
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException(
                "Invalid value for table.kafka.enabled: '"
                        + value
                        + "'. Expected true or false (case insensitive).");
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

    /** Returns whether a name is reserved at the physical table's top level. */
    public static boolean isReservedName(String name) {
        return RESERVED_NAMES.contains(name);
    }

    /** Resolves an explicitly enabled table or rejects its invalid contract. */
    public static KafkaTableSchema resolve(TableDescriptor table) {
        if (!isEnabled(table)) {
            throw invalid("Kafka compatibility is disabled; table.kafka.enabled must be true.");
        }
        // The initial Kafka write path encodes Arrow batches and appends them to log tables.
        // Other log formats require matching encoders before they can be enabled here.
        if (table.hasPrimaryKey()
                || table.isPartitioned()
                || new TableConfig(Configuration.fromMap(table.getProperties())).getLogFormat()
                        != LogFormat.ARROW) {
            throw invalid(
                    "Kafka requires a non-partitioned Arrow log table without a primary key.");
        }
        Configuration options = Configuration.fromMap(table.getProperties());
        KafkaValueConversionType valueConversionType =
                options.get(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE);
        KafkaValueConversionLayout valueConversionLayout =
                valueConversionType == KafkaValueConversionType.PARSE_JSON
                        ? options.get(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT)
                        : null;

        RowType rowType = table.getSchema().getRowType();
        int keyPosition = validateSystemColumnType(rowType, KEY_COLUMN, KEY_TYPE);
        if (options.getOptional(ConfigOptions.TABLE_KAFKA_CONVERT_KEY_TYPE).isPresent()
                && keyPosition < 0) {
            throw invalid("table.kafka.convert.key.type requires _kafka_key BYTES.");
        }
        int headersPosition = validateSystemColumnType(rowType, HEADERS_COLUMN, HEADERS_TYPE);
        int rescuePosition = validateSystemColumnType(rowType, RESCUE_COLUMN, RESCUE_TYPE);
        int metadataPosition = validateSystemColumnType(rowType, METADATA_COLUMN, METADATA_TYPE);

        List<Integer> valuePositions = new ArrayList<>();
        if (valueConversionLayout == KafkaValueConversionLayout.FLAT) {
            if (rowType.getFieldIndex(VALUE_COLUMN) >= 0) {
                throw invalid(
                        "_kafka_value is reserved for binary, string or nested JSON; "
                                + "it cannot be a flat JSON business field.");
            }
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (!isReservedName(rowType.getFields().get(i).getName())) {
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
                    valueConversionType == KafkaValueConversionType.BINARY
                            ? equalsIgnoreFieldDescriptionsAndIds(BINARY_VALUE_TYPE, type)
                            : valueConversionType == KafkaValueConversionType.STRING
                                    ? equalsIgnoreFieldDescriptionsAndIds(STRING_VALUE_TYPE, type)
                                    : type instanceof RowType;
            if (!valid) {
                throw invalid(
                        "_kafka_value must be "
                                + (valueConversionType == KafkaValueConversionType.BINARY
                                        ? "BYTES"
                                        : valueConversionType == KafkaValueConversionType.STRING
                                                ? "STRING"
                                                : "ROW")
                                + " for "
                                + valueConversionType
                                + ", but was "
                                + type.asSummaryString()
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
            valuePositions.add(valuePosition);
        }
        if (valueConversionType == KafkaValueConversionType.PARSE_JSON) {
            for (int position : valuePositions) {
                DataType type = rowType.getTypeAt(position);
                if (valueConversionLayout == KafkaValueConversionLayout.NESTED) {
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
        for (DataField field : rowType.getFields()) {
            validateNullable(field.getType(), field.getName());
        }
        return new KafkaTableSchema(
                rowType,
                valueConversionType,
                valueConversionLayout,
                valuePositions,
                keyPosition,
                headersPosition,
                metadataPosition,
                rescuePosition);
    }

    /**
     * Validates a top-level JSON business field without parser or protocol dependencies. DDL and
     * decoder construction share this contract. All currently defined Fluss types are supported
     * except MAP types with non-STRING keys (including nested maps) and schemas nested deeper than
     * 64 levels. Each business field starts at depth zero; ROW fields, ARRAY elements and MAP
     * values each advance the depth by one. Table resolution separately requires nullable fields;
     * scalar value constraints are enforced when decoding individual records. The JSON object root
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

    private static void validateNullable(DataType type, String path) {
        if (!type.isNullable()) {
            throw invalid("Kafka requires nullable fields, but '" + path + "' is NOT NULL.");
        }
        if (type instanceof RowType) {
            for (DataField field : ((RowType) type).getFields()) {
                validateNullable(field.getType(), path + "." + field.getName());
            }
        } else if (type instanceof ArrayType) {
            validateNullable(((ArrayType) type).getElementType(), path + "[]");
        } else if (type instanceof MapType) {
            // Fluss requires non-null MAP keys. Only values can be nulled by field-level rescue.
            validateNullable(((MapType) type).getValueType(), path + "<value>");
        }
    }

    private static int validateSystemColumnType(
            RowType rowType, String columnName, DataType expectedType) {
        int position = rowType.getFieldIndex(columnName);
        if (position >= 0) {
            DataType type = rowType.getTypeAt(position);
            if (!equalsIgnoreFieldDescriptionsAndIds(expectedType, type)) {
                throw invalid(
                        columnName
                                + " must be "
                                + expectedType.asSummaryString()
                                + ", but was "
                                + type.asSummaryString()
                                + ".");
            }
        }
        return position;
    }

    /** Returns the unchanged physical schema, including declared nullability. */
    public RowType rowType() {
        return rowType;
    }

    /** Returns the value conversion type. */
    public KafkaValueConversionType valueConversionType() {
        return valueConversionType;
    }

    /** Returns the JSON layout, or null for binary/string. */
    public @Nullable KafkaValueConversionLayout valueConversionLayout() {
        return valueConversionLayout;
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
     * reserved members and values that fail conversion here; binary/string decoders write null to
     * this optional column.
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
                && valueConversionType == that.valueConversionType
                && valueConversionLayout == that.valueConversionLayout
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
                valueConversionType,
                valueConversionLayout,
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
