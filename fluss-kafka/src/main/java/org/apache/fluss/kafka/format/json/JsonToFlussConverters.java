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

import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.fluss.utils.DateTimeUtils.timestampToMicros;
import static org.apache.fluss.utils.DateTimeUtils.timestampToNanos;

/**
 * JSON-to-Fluss converter construction.
 *
 * <p>Scalar coercion and recursive conversion follow Apache Flink 1.20's {@code
 * JsonToRowDataConverters}, adapted to Fluss internal values and schema nullability. Fluss
 * additionally normalizes temporal precision and fixed binary widths, supports timestamp offsets
 * and local timestamps, and reports numeric overflow instead of storing non-finite numbers or a
 * null decimal.
 */
final class JsonToFlussConverters {

    private JsonToFlussConverters() {}

    /**
     * Creates a converter for one top-level JSON business field, excluding the JSON root container.
     * The shared schema contract validates the entire type before recursive converter construction.
     */
    static JsonToFlussConverter create(DataType dataType) {
        try {
            KafkaTableSchema.validateJsonType(dataType);
        } catch (InvalidTableException e) {
            throw new KafkaTopicSchemaException(e.getMessage());
        }
        return createValidated(dataType);
    }

    private static JsonToFlussConverter createValidated(DataType dataType) {
        final JsonToFlussConverter notNullConverter = createNotNull(dataType);
        return (node, path, rescue) -> {
            if (node == null || node.isNull() || node.isMissingNode()) {
                if (!dataType.isNullable()) {
                    throw invalid(path, dataType, "field is missing or null").recordException();
                }
                return null;
            }
            try {
                return notNullConverter.convert(node, path, rescue);
            } catch (KafkaRecordEncodingException e) {
                throw e;
            } catch (ConversionException | NumberFormatException e) {
                if (rescue != null && dataType.isNullable()) {
                    rescue.accept(node);
                    return null;
                }
                throw e instanceof ConversionException
                        ? ((ConversionException) e).recordException()
                        : invalid(path, dataType, "value cannot be converted", e).recordException();
            } catch (RuntimeException e) {
                // Internal errors must not silently turn valid data into null.
                throw invalid(path, dataType, "value cannot be converted", e).recordException();
            }
        };
    }

    private static JsonToFlussConverter createNotNull(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return booleanConverter();
            case TINYINT:
                return tinyIntConverter();
            case SMALLINT:
                return smallIntConverter();
            case INTEGER:
                return intConverter();
            case BIGINT:
                return bigIntConverter();
            case FLOAT:
                return floatConverter(dataType);
            case DOUBLE:
                return doubleConverter(dataType);
            case DECIMAL:
                return decimalConverter((DecimalType) dataType);
            case CHAR:
            case STRING:
                return stringConverter();
            case BINARY:
                return binaryConverter((BinaryType) dataType);
            case BYTES:
                return bytesConverter(dataType);
            case DATE:
                return dateConverter(dataType);
            case TIME_WITHOUT_TIME_ZONE:
                return timeConverter((TimeType) dataType);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampConverter((TimestampType) dataType);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampLtzConverter((LocalZonedTimestampType) dataType);
            case ARRAY:
                return arrayConverter((ArrayType) dataType);
            case MAP:
                return mapConverter((MapType) dataType);
            case ROW:
                return rowConverter((RowType) dataType);
            default:
                throw new KafkaTopicSchemaException(
                        "Kafka JSON format does not support Fluss type "
                                + dataType.asSummaryString()
                                + ".");
        }
    }

    private static JsonToFlussConverter booleanConverter() {
        // parseBoolean treats every non-true string as false.
        return (node, path, rescue) ->
                node.isBoolean() ? node.booleanValue() : Boolean.parseBoolean(node.asText().trim());
    }

    private static JsonToFlussConverter tinyIntConverter() {
        return (node, path, rescue) -> Byte.parseByte(node.asText().trim());
    }

    private static JsonToFlussConverter smallIntConverter() {
        return (node, path, rescue) -> Short.parseShort(node.asText().trim());
    }

    private static JsonToFlussConverter intConverter() {
        // Flink truncates fractional numeric nodes, but parses text as an exact integer.
        return (node, path, rescue) ->
                node.canConvertToInt() ? node.asInt() : Integer.parseInt(node.asText().trim());
    }

    private static JsonToFlussConverter bigIntConverter() {
        return (node, path, rescue) ->
                node.canConvertToLong() ? node.asLong() : Long.parseLong(node.asText().trim());
    }

    private static JsonToFlussConverter floatConverter(DataType dataType) {
        return (node, path, rescue) -> {
            // Parse decimal text directly to float to avoid an intermediate double rounding.
            float value =
                    node.isDouble()
                            ? (float) node.asDouble()
                            : Float.parseFloat(node.asText().trim());
            require(Float.isFinite(value), path, dataType, "non-finite or overflowing number");
            return value;
        };
    }

    private static JsonToFlussConverter doubleConverter(DataType dataType) {
        return (node, path, rescue) -> {
            double value =
                    node.isDouble() ? node.asDouble() : Double.parseDouble(node.asText().trim());
            require(Double.isFinite(value), path, dataType, "non-finite or overflowing number");
            return value;
        };
    }

    private static JsonToFlussConverter decimalConverter(DecimalType dataType) {
        return (node, path, rescue) -> {
            BigDecimal value =
                    node.isBigDecimal() ? node.decimalValue() : new BigDecimal(node.asText());
            if (value.signum() == 0) {
                return Decimal.zero(dataType.getPrecision(), dataType.getScale());
            }
            // Check the resulting digits before setScale can expand a large JSON exponent.
            // Use long arithmetic because BigDecimal scales may span the entire int range.
            long scaledPrecision = (long) value.precision() - value.scale() + dataType.getScale();
            require(
                    scaledPrecision <= dataType.getPrecision(),
                    path,
                    dataType,
                    "decimal precision exceeds target");
            // Values below a tenth of the target unit round to zero. Avoid constructing a huge
            // power of ten when the input has an extreme negative exponent.
            if (scaledPrecision < 0) {
                return Decimal.zero(dataType.getPrecision(), dataType.getScale());
            }
            // Decimal uses HALF_UP and checks precision again after a possible rounding carry.
            Decimal decimal =
                    Decimal.fromBigDecimal(value, dataType.getPrecision(), dataType.getScale());
            if (decimal == null) {
                throw invalid(path, dataType, "decimal precision exceeds target");
            }
            return decimal;
        };
    }

    private static JsonToFlussConverter stringConverter() {
        return (node, path, rescue) ->
                BinaryString.fromString(node.isContainerNode() ? node.toString() : node.asText());
    }

    private static JsonToFlussConverter binaryConverter(BinaryType dataType) {
        return (node, path, rescue) -> {
            byte[] value = decodeBase64(node, path, dataType);
            // Arrow uses FixedSizeBinaryVector: zero-pad short values and truncate long ones.
            return value.length == dataType.getLength()
                    ? value
                    : Arrays.copyOf(value, dataType.getLength());
        };
    }

    private static JsonToFlussConverter bytesConverter(DataType dataType) {
        return (node, path, rescue) -> decodeBase64(node, path, dataType);
    }

    private static JsonToFlussConverter dateConverter(DataType dataType) {
        return (node, path, rescue) -> {
            String value = textualValue(node, path, dataType);
            try {
                return Math.toIntExact(LocalDate.parse(value).toEpochDay());
            } catch (DateTimeException | ArithmeticException e) {
                throw invalid(path, dataType, "invalid ISO-8601 date", e);
            }
        };
    }

    private static JsonToFlussConverter timeConverter(TimeType dataType) {
        final int nanosPerUnit = nanosPerUnit(Math.min(dataType.getPrecision(), 3));
        return (node, path, rescue) -> {
            String value = textualValue(node, path, dataType);
            final LocalTime time;
            try {
                time = LocalTime.parse(value);
            } catch (DateTimeParseException e) {
                throw invalid(path, dataType, "invalid ISO-8601 local time", e);
            }
            return time.toSecondOfDay() * 1_000
                    + (time.getNano() / nanosPerUnit * nanosPerUnit) / 1_000_000;
        };
    }

    private static JsonToFlussConverter timestampConverter(TimestampType dataType) {
        final int nanosPerUnit = nanosPerUnit(dataType.getPrecision());
        return (node, path, rescue) -> {
            String value = textualValue(node, path, dataType).replace(' ', 'T');
            final LocalDateTime timestamp;
            try {
                timestamp = LocalDateTime.parse(value);
            } catch (DateTimeParseException e) {
                throw invalid(path, dataType, "invalid ISO-8601 local timestamp", e);
            }
            LocalDateTime normalized =
                    timestamp.withNano(timestamp.getNano() / nanosPerUnit * nanosPerUnit);
            long millis =
                    timestampMillis(
                            normalized.toInstant(ZoneOffset.UTC),
                            dataType.getPrecision(),
                            path,
                            dataType);
            return TimestampNtz.fromMillis(millis, normalized.getNano() % 1_000_000);
        };
    }

    private static JsonToFlussConverter timestampLtzConverter(LocalZonedTimestampType dataType) {
        final int nanosPerUnit = nanosPerUnit(dataType.getPrecision());
        final ZoneId localZone = ZoneId.systemDefault();
        return (node, path, rescue) -> {
            String value = textualValue(node, path, dataType).replace(' ', 'T');
            Instant timestamp;
            try {
                timestamp = OffsetDateTime.parse(value).toInstant();
            } catch (DateTimeParseException e) {
                try {
                    timestamp = LocalDateTime.parse(value).atZone(localZone).toInstant();
                } catch (DateTimeParseException localError) {
                    throw invalid(path, dataType, "invalid timestamp", localError);
                }
            }
            Instant normalized =
                    Instant.ofEpochSecond(
                            timestamp.getEpochSecond(),
                            timestamp.getNano() / nanosPerUnit * nanosPerUnit);
            long millis = timestampMillis(normalized, dataType.getPrecision(), path, dataType);
            return TimestampLtz.fromEpochMillis(millis, normalized.getNano() % 1_000_000);
        };
    }

    private static JsonToFlussConverter arrayConverter(ArrayType dataType) {
        JsonToFlussConverter elementConverter = createValidated(dataType.getElementType());
        return (node, path, rescue) -> {
            require(node.isArray(), path, dataType, "expected a JSON array");
            RescueCollector collector = rescue == null ? null : new RescueCollector(rescue);
            Object[] elements = new Object[node.size()];
            for (int i = 0; i < elements.length; i++) {
                elements[i] =
                        elementConverter.convert(
                                node.get(i),
                                JsonPath.index(path, i),
                                collector == null ? null : collector.element(i, elements.length));
            }
            if (collector != null) {
                collector.finish();
            }
            return new GenericArray(elements);
        };
    }

    private static JsonToFlussConverter mapConverter(MapType dataType) {
        JsonToFlussConverter valueConverter = createValidated(dataType.getValueType());
        return (node, path, rescue) -> {
            require(node.isObject(), path, dataType, "expected a JSON object");
            RescueCollector collector = rescue == null ? null : new RescueCollector(rescue);
            Map<BinaryString, Object> values = new LinkedHashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                values.put(
                        BinaryString.fromString(field.getKey()),
                        valueConverter.convert(
                                field.getValue(),
                                JsonPath.field(path, field.getKey()),
                                collector == null ? null : collector.field(field.getKey())));
            }
            if (collector != null) {
                collector.finish();
            }
            return new GenericMap(values);
        };
    }

    private static JsonToFlussConverter rowConverter(RowType dataType) {
        List<DataField> fields = dataType.getFields();
        String[] fieldNames = new String[fields.size()];
        JsonToFlussConverter[] fieldConverters = new JsonToFlussConverter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            fieldNames[i] = field.getName();
            fieldConverters[i] = createValidated(field.getType());
        }
        Set<String> knownFields = new HashSet<>(Arrays.asList(fieldNames));
        return (node, path, rescue) -> {
            require(node.isObject(), path, dataType, "expected a JSON object");
            RescueCollector collector = rescue == null ? null : new RescueCollector(rescue);
            if (collector != null) {
                Iterator<Map.Entry<String, JsonNode>> members = node.fields();
                while (members.hasNext()) {
                    Map.Entry<String, JsonNode> member = members.next();
                    if (!knownFields.contains(member.getKey())) {
                        collector.addField(member.getKey(), member.getValue());
                    }
                }
            }
            GenericRow row = new GenericRow(fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {
                row.setField(
                        i,
                        fieldConverters[i].convert(
                                node.get(fieldNames[i]),
                                JsonPath.field(path, fieldNames[i]),
                                collector == null ? null : collector.field(fieldNames[i])));
            }
            if (collector != null) {
                collector.finish();
            }
            return row;
        };
    }

    /** Collects a container's unknown members and failed values without changing the input tree. */
    private static final class RescueCollector {
        private final Consumer<JsonNode> sink;
        @Nullable private JsonNode rescued;

        private RescueCollector(Consumer<JsonNode> sink) {
            this.sink = sink;
        }

        private Consumer<JsonNode> field(String name) {
            return value -> addField(name, value);
        }

        private void addField(String name, JsonNode value) {
            if (rescued == null) {
                rescued = JsonNodeFactory.instance.objectNode();
            }
            ((ObjectNode) rescued).set(name, value);
        }

        private Consumer<JsonNode> element(int index, int size) {
            return value -> {
                if (rescued == null) {
                    ArrayNode array = JsonNodeFactory.instance.arrayNode(size);
                    // Keep original indices so rescued values can be matched to array elements.
                    for (int i = 0; i < size; i++) {
                        array.addNull();
                    }
                    rescued = array;
                }
                ((ArrayNode) rescued).set(index, value);
            };
        }

        private void finish() {
            if (rescued != null) {
                sink.accept(rescued);
            }
        }
    }

    private static long timestampMillis(
            Instant timestamp, int precision, String path, DataType dataType) {
        try {
            long millis = timestamp.toEpochMilli();
            int nanos = timestamp.getNano() % 1_000_000;
            // Arrow stores precision 4-6 in microseconds and precision 7-9 in nanoseconds.
            // Check here as well so Kafka reports the offending JSON field before encoding.
            if (precision > 6) {
                timestampToNanos(millis, nanos);
            } else if (precision > 3) {
                timestampToMicros(millis, nanos);
            }
            return millis;
        } catch (ArithmeticException e) {
            throw invalid(path, dataType, "timestamp exceeds storage range", e);
        }
    }

    private static String textualValue(JsonNode node, String path, DataType dataType) {
        require(node.isTextual(), path, dataType, "expected a JSON string");
        return node.textValue();
    }

    private static byte[] decodeBase64(JsonNode node, String path, DataType dataType) {
        require(
                node.isTextual() || node.isBinary(),
                path,
                dataType,
                "expected a Base64 string or binary node");
        try {
            return node.binaryValue();
        } catch (IOException e) {
            throw invalid(path, dataType, "invalid Base64 string", e);
        }
    }

    private static int nanosPerUnit(int precision) {
        int unit = 1;
        for (int i = precision; i < 9; i++) {
            unit *= 10;
        }
        return unit;
    }

    private static void require(boolean condition, String path, DataType dataType, String reason) {
        if (!condition) {
            throw invalid(path, dataType, reason);
        }
    }

    private static ConversionException invalid(String path, DataType dataType, String reason) {
        return invalid(path, dataType, reason, null);
    }

    private static ConversionException invalid(
            String path, DataType dataType, String reason, @Nullable Throwable cause) {
        return new ConversionException(
                "Invalid Kafka record value at "
                        + path
                        + " for "
                        + dataType.asSummaryString()
                        + ": "
                        + reason
                        + ".",
                cause);
    }

    /** Distinguishes expected data conversion failures from internal errors. */
    private static final class ConversionException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private ConversionException(String message, @Nullable Throwable cause) {
            super(message, cause);
        }

        private KafkaRecordEncodingException recordException() {
            return new KafkaRecordEncodingException(getMessage(), getCause());
        }
    }
}
