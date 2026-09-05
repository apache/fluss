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

package org.apache.fluss.kafka.format.json;

import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Strict recursive JSON-to-Fluss converter construction. */
final class JsonToFlussConverters {

    static final int MAX_SCHEMA_NESTING_DEPTH = 64;
    static final int MAX_CONTAINER_ELEMENTS = 10_000;

    private JsonToFlussConverters() {}

    static JsonToFlussConverter create(DataType dataType) {
        return create(dataType, 0);
    }

    private static JsonToFlussConverter create(DataType dataType, int nestingDepth) {
        if (nestingDepth > MAX_SCHEMA_NESTING_DEPTH) {
            throw new KafkaTopicSchemaException(
                    "Kafka JSON schema exceeds the maximum nesting depth of "
                            + MAX_SCHEMA_NESTING_DEPTH
                            + ".");
        }
        final JsonToFlussConverter notNullConverter = createNotNull(dataType, nestingDepth);
        return (node, path) -> {
            if (node == null || node.isNull()) {
                if (!dataType.isNullable()) {
                    throw invalid(path, dataType, "field is missing or null");
                }
                return null;
            }
            try {
                return notNullConverter.convert(node, path);
            } catch (KafkaRecordEncodingException e) {
                throw e;
            } catch (RuntimeException e) {
                throw invalid(path, dataType, "value cannot be converted", e);
            }
        };
    }

    private static JsonToFlussConverter createNotNull(DataType dataType, int nestingDepth) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return (node, path) -> {
                    require(node.isBoolean(), path, dataType, "expected a JSON boolean");
                    return node.booleanValue();
                };
            case TINYINT:
                return (node, path) -> {
                    long value = integralValue(node, path, dataType);
                    require(
                            value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE,
                            path,
                            dataType,
                            "integer overflow");
                    return (byte) value;
                };
            case SMALLINT:
                return (node, path) -> {
                    long value = integralValue(node, path, dataType);
                    require(
                            value >= Short.MIN_VALUE && value <= Short.MAX_VALUE,
                            path,
                            dataType,
                            "integer overflow");
                    return (short) value;
                };
            case INTEGER:
                return (node, path) -> {
                    long value = integralValue(node, path, dataType);
                    require(
                            value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE,
                            path,
                            dataType,
                            "integer overflow");
                    return (int) value;
                };
            case BIGINT:
                return (node, path) -> integralValue(node, path, dataType);
            case FLOAT:
                return (node, path) -> {
                    require(node.isNumber(), path, dataType, "expected a JSON number");
                    double doubleValue = node.doubleValue();
                    float value = (float) doubleValue;
                    require(
                            Double.isFinite(doubleValue) && Float.isFinite(value),
                            path,
                            dataType,
                            "non-finite or overflowing number");
                    return value;
                };
            case DOUBLE:
                return (node, path) -> {
                    require(node.isNumber(), path, dataType, "expected a JSON number");
                    double value = node.doubleValue();
                    require(
                            Double.isFinite(value),
                            path,
                            dataType,
                            "non-finite or overflowing number");
                    return value;
                };
            case DECIMAL:
                return decimalConverter((DecimalType) dataType);
            case CHAR:
                return charConverter((CharType) dataType);
            case STRING:
                return (node, path) -> {
                    require(node.isTextual(), path, dataType, "expected a JSON string");
                    return BinaryString.fromString(node.textValue());
                };
            case BINARY:
                return binaryConverter((BinaryType) dataType);
            case BYTES:
                return (node, path) -> decodeBase64(node, path, dataType);
            case DATE:
                return (node, path) -> {
                    String value = textualValue(node, path, dataType);
                    try {
                        return Math.toIntExact(LocalDate.parse(value).toEpochDay());
                    } catch (DateTimeException | ArithmeticException e) {
                        throw invalid(path, dataType, "invalid ISO-8601 date", e);
                    }
                };
            case TIME_WITHOUT_TIME_ZONE:
                return timeConverter((TimeType) dataType);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampConverter((TimestampType) dataType);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampLtzConverter((LocalZonedTimestampType) dataType);
            case ROW:
                return rowConverter((RowType) dataType, nestingDepth);
            case ARRAY:
                return arrayConverter((ArrayType) dataType, nestingDepth);
            case MAP:
                return mapConverter((MapType) dataType, nestingDepth);
            default:
                throw new KafkaTopicSchemaException(
                        "Kafka JSON format does not support Fluss type "
                                + dataType.asSummaryString()
                                + ".");
        }
    }

    private static JsonToFlussConverter decimalConverter(DecimalType dataType) {
        return (node, path) -> {
            require(node.isNumber(), path, dataType, "expected a JSON number");
            BigDecimal value = node.decimalValue();
            final BigDecimal scaled;
            try {
                scaled = value.setScale(dataType.getScale(), RoundingMode.UNNECESSARY);
            } catch (ArithmeticException e) {
                throw invalid(path, dataType, "decimal scale exceeds target", e);
            }
            Decimal decimal =
                    Decimal.fromBigDecimal(scaled, dataType.getPrecision(), dataType.getScale());
            if (decimal == null) {
                throw invalid(path, dataType, "decimal precision exceeds target");
            }
            return decimal;
        };
    }

    private static JsonToFlussConverter charConverter(CharType dataType) {
        return (node, path) -> {
            String value = textualValue(node, path, dataType);
            int codePoints = value.codePointCount(0, value.length());
            require(
                    codePoints <= dataType.getLength(),
                    path,
                    dataType,
                    "character length exceeds target");
            return BinaryString.fromString(value);
        };
    }

    private static JsonToFlussConverter binaryConverter(BinaryType dataType) {
        return (node, path) -> {
            byte[] value = decodeBase64(node, path, dataType);
            require(
                    value.length == dataType.getLength(),
                    path,
                    dataType,
                    "decoded binary length must equal " + dataType.getLength());
            return value;
        };
    }

    private static JsonToFlussConverter timeConverter(TimeType dataType) {
        return (node, path) -> {
            String value = textualValue(node, path, dataType);
            final LocalTime time;
            try {
                time = LocalTime.parse(value);
            } catch (DateTimeParseException e) {
                throw invalid(path, dataType, "invalid ISO-8601 local time", e);
            }
            validatePrecision(time.getNano(), dataType.getPrecision(), path, dataType);
            require(
                    time.getNano() % 1_000_000 == 0,
                    path,
                    dataType,
                    "Fluss TIME storage currently requires millisecond precision");
            return Math.toIntExact(time.toNanoOfDay() / 1_000_000);
        };
    }

    private static JsonToFlussConverter timestampConverter(TimestampType dataType) {
        return (node, path) -> {
            String value = textualValue(node, path, dataType);
            final LocalDateTime timestamp;
            try {
                timestamp = LocalDateTime.parse(value);
            } catch (DateTimeParseException e) {
                throw invalid(path, dataType, "invalid ISO-8601 local timestamp", e);
            }
            validatePrecision(timestamp.getNano(), dataType.getPrecision(), path, dataType);
            return TimestampNtz.fromLocalDateTime(timestamp);
        };
    }

    private static JsonToFlussConverter timestampLtzConverter(LocalZonedTimestampType dataType) {
        return (node, path) -> {
            String value = textualValue(node, path, dataType);
            final OffsetDateTime timestamp;
            try {
                timestamp = OffsetDateTime.parse(value);
            } catch (DateTimeParseException e) {
                throw invalid(
                        path,
                        dataType,
                        "invalid ISO-8601 timestamp; an offset or Z is required",
                        e);
            }
            validatePrecision(timestamp.getNano(), dataType.getPrecision(), path, dataType);
            return TimestampLtz.fromInstant(timestamp.toInstant());
        };
    }

    private static JsonToFlussConverter rowConverter(RowType dataType, int nestingDepth) {
        List<DataField> fields = dataType.getFields();
        String[] fieldNames = new String[fields.size()];
        Set<String> declaredFieldNames = new HashSet<>();
        JsonToFlussConverter[] fieldConverters = new JsonToFlussConverter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            fieldNames[i] = field.getName();
            declaredFieldNames.add(field.getName());
            fieldConverters[i] = create(field.getType(), nestingDepth + 1);
        }
        return (node, path) -> {
            require(node.isObject(), path, dataType, "expected a JSON object");
            requireContainerSize(node.size(), path, dataType);
            Iterator<String> inputFieldNames = node.fieldNames();
            while (inputFieldNames.hasNext()) {
                String inputFieldName = inputFieldNames.next();
                require(
                        declaredFieldNames.contains(inputFieldName),
                        JsonPath.field(path, inputFieldName),
                        dataType,
                        "unknown field");
            }
            GenericRow row = new GenericRow(fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {
                String fieldPath = JsonPath.field(path, fieldNames[i]);
                row.setField(i, fieldConverters[i].convert(node.get(fieldNames[i]), fieldPath));
            }
            return row;
        };
    }

    private static JsonToFlussConverter arrayConverter(ArrayType dataType, int nestingDepth) {
        JsonToFlussConverter elementConverter = create(dataType.getElementType(), nestingDepth + 1);
        return (node, path) -> {
            require(node.isArray(), path, dataType, "expected a JSON array");
            requireContainerSize(node.size(), path, dataType);
            Object[] elements = new Object[node.size()];
            for (int i = 0; i < node.size(); i++) {
                elements[i] = elementConverter.convert(node.get(i), JsonPath.index(path, i));
            }
            return new GenericArray(elements);
        };
    }

    private static JsonToFlussConverter mapConverter(MapType dataType, int nestingDepth) {
        if (dataType.getKeyType().getTypeRoot() != DataTypeRoot.STRING) {
            throw new KafkaTopicSchemaException(
                    "Kafka JSON format only supports STRING map keys, but found "
                            + dataType.asSummaryString()
                            + ".");
        }
        JsonToFlussConverter valueConverter = create(dataType.getValueType(), nestingDepth + 1);
        return (node, path) -> {
            require(node.isObject(), path, dataType, "expected a JSON object");
            requireContainerSize(node.size(), path, dataType);
            Map<BinaryString, Object> values = new LinkedHashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                values.put(
                        BinaryString.fromString(field.getKey()),
                        valueConverter.convert(
                                field.getValue(), JsonPath.field(path, field.getKey())));
            }
            return new GenericMap(values);
        };
    }

    private static void requireContainerSize(int size, String path, DataType dataType) {
        require(
                size <= MAX_CONTAINER_ELEMENTS,
                path,
                dataType,
                "container size exceeds " + MAX_CONTAINER_ELEMENTS);
    }

    private static long integralValue(JsonNode node, String path, DataType dataType) {
        require(node.isIntegralNumber(), path, dataType, "expected a JSON integer");
        require(node.canConvertToLong(), path, dataType, "integer overflow");
        return node.longValue();
    }

    private static String textualValue(JsonNode node, String path, DataType dataType) {
        require(node.isTextual(), path, dataType, "expected a JSON string");
        return node.textValue();
    }

    private static byte[] decodeBase64(JsonNode node, String path, DataType dataType) {
        String value = textualValue(node, path, dataType);
        try {
            return Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            throw invalid(path, dataType, "invalid Base64 string", e);
        }
    }

    private static void validatePrecision(
            int nanos, int precision, String path, DataType dataType) {
        if (precision >= 9) {
            return;
        }
        int unit = 1;
        for (int i = precision; i < 9; i++) {
            unit *= 10;
        }
        require(nanos % unit == 0, path, dataType, "fractional seconds exceed target precision");
    }

    private static void require(boolean condition, String path, DataType dataType, String reason) {
        if (!condition) {
            throw invalid(path, dataType, reason);
        }
    }

    private static KafkaRecordEncodingException invalid(
            String path, DataType dataType, String reason) {
        return new KafkaRecordEncodingException(
                "Invalid Kafka record value at "
                        + path
                        + " for "
                        + dataType.asSummaryString()
                        + ": "
                        + reason
                        + ".");
    }

    private static KafkaRecordEncodingException invalid(
            String path, DataType dataType, String reason, @Nullable Throwable cause) {
        return new KafkaRecordEncodingException(
                "Invalid Kafka record value at "
                        + path
                        + " for "
                        + dataType.asSummaryString()
                        + ": "
                        + reason
                        + ".",
                cause);
    }
}
