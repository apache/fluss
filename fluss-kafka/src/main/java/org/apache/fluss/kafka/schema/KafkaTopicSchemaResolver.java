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

package org.apache.fluss.kafka.schema;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Resolves and validates the Kafka record mapping stored in Fluss table custom properties. */
@Internal
public final class KafkaTopicSchemaResolver {

    private static final String INCLUDE_ALL = "ALL";
    private static final String INCLUDE_EXCEPT_KEY = "EXCEPT_KEY";

    /** Resolves one table's Kafka record mapping contract. */
    public KafkaTopicSchema resolve(TableInfo tableInfo) {
        validateTableKind(tableInfo);
        RowType rowType = tableInfo.getRowType();
        Map<String, String> properties = tableInfo.getCustomProperties().toMap();

        int timestampPosition =
                resolveOptionalPosition(
                        rowType, properties.get(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG));
        int headersPosition =
                resolveOptionalPosition(
                        rowType, properties.get(KafkaDataFormat.HEADERS_COLUMN_CONFIG));
        validateMetadataColumns(rowType, timestampPosition, headersPosition);

        String keyFormatValue = properties.get(KafkaDataFormat.KEY_FORMAT_CONFIG);
        KafkaDataFormat keyFormat =
                keyFormatValue == null ? null : KafkaDataFormat.parse(keyFormatValue);
        if (keyFormat == KafkaDataFormat.JSON) {
            throw invalid("Kafka JSON keys are not supported.");
        }
        List<Integer> keyPositions =
                resolveKeyPositions(
                        rowType, keyFormat, properties.get(KafkaDataFormat.KEY_FIELDS_CONFIG));
        for (Integer keyPosition : keyPositions) {
            if (keyPosition == timestampPosition || keyPosition == headersPosition) {
                throw invalid(
                        "Kafka key field '"
                                + rowType.getFieldNames().get(keyPosition)
                                + "' cannot be a Kafka metadata column.");
            }
        }
        KafkaFieldProjection keyProjection = new KafkaFieldProjection(rowType, keyPositions);

        String valueFormatValue = properties.get(KafkaDataFormat.VALUE_FORMAT_CONFIG);
        if (valueFormatValue == null) {
            throw invalid(
                    "Missing required table property '"
                            + KafkaDataFormat.VALUE_FORMAT_CONFIG
                            + "'.");
        }
        KafkaDataFormat valueFormat = KafkaDataFormat.parse(valueFormatValue);
        String fieldsInclude =
                normalizeFieldsInclude(properties.get(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG));
        if (valueFormat == KafkaDataFormat.JSON
                && !keyPositions.isEmpty()
                && INCLUDE_ALL.equals(fieldsInclude)) {
            throw invalid(
                    "JSON value format with Kafka key fields requires "
                            + KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG
                            + "=EXCEPT_KEY.");
        }

        Set<Integer> metadataPositions = new HashSet<>();
        if (timestampPosition >= 0) {
            metadataPositions.add(timestampPosition);
        }
        if (headersPosition >= 0) {
            if (!metadataPositions.add(headersPosition)) {
                throw invalid("Kafka timestamp and headers cannot map to the same Fluss column.");
            }
        }
        int valueRescuePosition =
                resolveValueRescuePosition(
                        rowType,
                        valueFormat,
                        properties.get(KafkaDataFormat.VALUE_RESCUE_COLUMN_CONFIG),
                        keyPositions,
                        metadataPositions);
        List<Integer> valuePositions =
                resolveValuePositions(rowType, fieldsInclude, keyPositions, metadataPositions);
        KafkaFieldProjection valueProjection = new KafkaFieldProjection(rowType, valuePositions);
        if (valueProjection.isEmpty()) {
            throw invalid("Kafka value projection must contain at least one Fluss column.");
        }

        validateSingleFieldFormat(keyFormat, keyProjection, "key");
        validateSingleFieldFormat(valueFormat, valueProjection, "value");
        return new KafkaTopicSchema(
                rowType,
                keyFormat,
                keyProjection,
                valueFormat,
                valueProjection,
                valueRescuePosition < 0 ? null : rowType.getFieldNames().get(valueRescuePosition),
                timestampPosition,
                headersPosition);
    }

    private static void validateTableKind(TableInfo tableInfo) {
        if (tableInfo.hasPrimaryKey()) {
            throw invalid("Kafka topic table must be a log table.");
        }
        if (tableInfo.isPartitioned()) {
            throw invalid("Partitioned Fluss tables are not supported.");
        }
        if (tableInfo.getTableConfig().getLogFormat() != LogFormat.ARROW) {
            throw invalid("Kafka topic table must use the Arrow log format.");
        }
    }

    private static List<Integer> resolveKeyPositions(
            RowType rowType, KafkaDataFormat keyFormat, String keyFieldsValue) {
        if (keyFormat == null) {
            if (keyFieldsValue != null && !keyFieldsValue.trim().isEmpty()) {
                throw invalid(
                        KafkaDataFormat.KEY_FIELDS_CONFIG
                                + " requires "
                                + KafkaDataFormat.KEY_FORMAT_CONFIG
                                + ".");
            }
            return Collections.emptyList();
        }
        if (keyFieldsValue == null || keyFieldsValue.trim().isEmpty()) {
            throw invalid(
                    "Missing required table property '" + KafkaDataFormat.KEY_FIELDS_CONFIG + "'.");
        }
        String[] fieldNames = keyFieldsValue.split(",");
        List<Integer> positions = new ArrayList<>(fieldNames.length);
        Set<Integer> uniquePositions = new HashSet<>();
        for (String fieldNameValue : fieldNames) {
            String fieldName = fieldNameValue.trim();
            if (fieldName.isEmpty()) {
                throw invalid("Kafka key field names must not be empty.");
            }
            int position = rowType.getFieldIndex(fieldName);
            if (position < 0) {
                throw invalid("Kafka key field '" + fieldName + "' does not exist.");
            }
            if (!uniquePositions.add(position)) {
                throw invalid("Duplicate Kafka key field '" + fieldName + "'.");
            }
            positions.add(position);
        }
        return positions;
    }

    private static String normalizeFieldsInclude(String value) {
        String normalized = value == null ? INCLUDE_ALL : value.trim().toUpperCase(Locale.ROOT);
        if (!INCLUDE_ALL.equals(normalized) && !INCLUDE_EXCEPT_KEY.equals(normalized)) {
            throw invalid(
                    "Invalid "
                            + KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG
                            + " '"
                            + value
                            + "'. Expected ALL or EXCEPT_KEY.");
        }
        return normalized;
    }

    private static List<Integer> resolveValuePositions(
            RowType rowType,
            String fieldsInclude,
            List<Integer> keyPositions,
            Set<Integer> metadataPositions) {
        Set<Integer> excludedKeyPositions =
                INCLUDE_EXCEPT_KEY.equals(fieldsInclude)
                        ? new HashSet<>(keyPositions)
                        : Collections.<Integer>emptySet();
        List<Integer> positions = new ArrayList<>();
        for (int position = 0; position < rowType.getFieldCount(); position++) {
            if (!metadataPositions.contains(position) && !excludedKeyPositions.contains(position)) {
                positions.add(position);
            }
        }
        return positions;
    }

    private static int resolveOptionalPosition(RowType rowType, String fieldNameValue) {
        if (fieldNameValue == null || fieldNameValue.trim().isEmpty()) {
            return -1;
        }
        String fieldName = fieldNameValue.trim();
        int position = rowType.getFieldIndex(fieldName);
        if (position < 0) {
            throw invalid("Kafka metadata column '" + fieldName + "' does not exist.");
        }
        return position;
    }

    private static int resolveValueRescuePosition(
            RowType rowType,
            KafkaDataFormat valueFormat,
            String fieldNameValue,
            List<Integer> keyPositions,
            Set<Integer> metadataPositions) {
        if (fieldNameValue == null || fieldNameValue.trim().isEmpty()) {
            return -1;
        }
        if (valueFormat != KafkaDataFormat.JSON) {
            throw invalid(
                    KafkaDataFormat.VALUE_RESCUE_COLUMN_CONFIG
                            + " is only supported for Kafka JSON values.");
        }
        String fieldName = fieldNameValue.trim();
        int position = rowType.getFieldIndex(fieldName);
        if (position < 0) {
            throw invalid("Kafka value rescue column '" + fieldName + "' does not exist.");
        }
        if (keyPositions.contains(position) || metadataPositions.contains(position)) {
            throw invalid(
                    "Kafka value rescue column '"
                            + fieldName
                            + "' cannot be a Kafka key or metadata column.");
        }
        if (!(rowType.getTypeAt(position) instanceof StringType)
                || !rowType.getTypeAt(position).isNullable()) {
            throw invalid("Kafka value rescue column '" + fieldName + "' must be nullable STRING.");
        }
        return position;
    }

    private static void validateMetadataColumns(
            RowType rowType, int timestampPosition, int headersPosition) {
        if (timestampPosition >= 0) {
            if (!(rowType.getTypeAt(timestampPosition) instanceof LocalZonedTimestampType)
                    || rowType.getTypeAt(timestampPosition).isNullable()
                    || ((LocalZonedTimestampType) rowType.getTypeAt(timestampPosition))
                                    .getPrecision()
                            != 3) {
                throw invalid("Kafka timestamp column must be TIMESTAMP_LTZ(3) NOT NULL.");
            }
        }
        if (headersPosition >= 0) {
            validateHeadersType(rowType.getTypeAt(headersPosition));
        }
    }

    private static void validateHeadersType(org.apache.fluss.types.DataType dataType) {
        if (!(dataType instanceof ArrayType) || !dataType.isNullable()) {
            throw invalid(
                    "Kafka headers column must be nullable "
                            + "ARRAY<ROW<name STRING, value BYTES>>.");
        }
        ArrayType arrayType = (ArrayType) dataType;
        if (!(arrayType.getElementType() instanceof RowType)) {
            throw invalid("Kafka headers elements must be rows.");
        }
        RowType headerType = (RowType) arrayType.getElementType();
        if (!headerType.getFieldNames().equals(Arrays.asList("name", "value"))
                || !(headerType.getTypeAt(0) instanceof StringType)
                || !(headerType.getTypeAt(1) instanceof BytesType)
                || !headerType.getTypeAt(1).isNullable()) {
            throw invalid("Kafka headers elements must be ROW<name STRING, value BYTES>.");
        }
    }

    private static void validateSingleFieldFormat(
            KafkaDataFormat format, KafkaFieldProjection projection, String component) {
        if (format == null) {
            return;
        }
        if (format == KafkaDataFormat.RAW || format == KafkaDataFormat.STRING) {
            if (projection.size() != 1) {
                throw invalid(
                        "Kafka "
                                + component
                                + " format "
                                + format.value()
                                + " requires exactly one Fluss field.");
            }
            boolean validType =
                    format == KafkaDataFormat.RAW
                            ? projection.dataTypeAt(0) instanceof BytesType
                            : projection.dataTypeAt(0) instanceof StringType;
            if (!validType) {
                throw invalid(
                        "Kafka "
                                + component
                                + " field '"
                                + projection.nameAt(0)
                                + "' must be "
                                + (format == KafkaDataFormat.RAW ? "BYTES" : "STRING")
                                + " for format "
                                + format.value()
                                + ".");
            }
        }
    }

    private static KafkaTopicSchemaException invalid(String message) {
        return new KafkaTopicSchemaException(message);
    }
}
