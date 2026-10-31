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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
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
import java.io.StringWriter;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.DateTimeUtils.timestampToMicros;
import static org.apache.fluss.utils.DateTimeUtils.timestampToNanos;

/** Schema-compiled converters that consume one complete value from a Jackson parser. */
final class JsonParserToFlussConverters {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final BigDecimal MIN_INT = BigDecimal.valueOf(Integer.MIN_VALUE);
    private static final BigDecimal MAX_INT = BigDecimal.valueOf(Integer.MAX_VALUE);
    private static final BigDecimal MIN_LONG = BigDecimal.valueOf(Long.MIN_VALUE);
    private static final BigDecimal MAX_LONG = BigDecimal.valueOf(Long.MAX_VALUE);

    private JsonParserToFlussConverters() {}

    /** A converter consumes the parser's current value and leaves it on that value's last token. */
    interface Converter {
        @Nullable
        Object convert(JsonParser parser, String path, @Nullable RescueSink rescue)
                throws IOException;

        @Nullable
        Object convertMissing(String path);
    }

    /** Receives a rescued value without imposing Jackson's numeric-node normalization. */
    interface RescueSink {
        void accept(RescueValue value);
    }

    abstract static class RescueValue {
        abstract void write(JsonGenerator generator) throws IOException;

        final String toJson(ObjectMapper mapper) throws IOException {
            StringWriter writer = new StringWriter();
            try (JsonGenerator generator = mapper.getFactory().createGenerator(writer)) {
                write(generator);
            }
            return writer.toString();
        }
    }

    static final class ObjectRescue extends RescueValue {
        private final Map<String, RescueValue> fields = new LinkedHashMap<>();

        void add(String name, RescueValue value) {
            fields.put(name, value);
        }

        boolean isEmpty() {
            return fields.isEmpty();
        }

        @Override
        void write(JsonGenerator generator) throws IOException {
            generator.writeStartObject();
            for (Map.Entry<String, RescueValue> field : fields.entrySet()) {
                generator.writeFieldName(field.getKey());
                field.getValue().write(generator);
            }
            generator.writeEndObject();
        }
    }

    private static final class ArrayRescue extends RescueValue {
        private final List<RescueValue> elements = new ArrayList<>();

        private void set(int index, RescueValue value) {
            ensureSize(index + 1);
            elements.set(index, value);
        }

        private void ensureSize(int size) {
            while (elements.size() < size) {
                elements.add(NullRescue.INSTANCE);
            }
        }

        @Override
        void write(JsonGenerator generator) throws IOException {
            generator.writeStartArray();
            for (RescueValue element : elements) {
                element.write(generator);
            }
            generator.writeEndArray();
        }
    }

    private static final class StringRescue extends RescueValue {
        private final String value;

        private StringRescue(String value) {
            this.value = value;
        }

        @Override
        void write(JsonGenerator generator) throws IOException {
            generator.writeString(value);
        }
    }

    private static final class NumberRescue extends RescueValue {
        private final String token;

        private NumberRescue(String token) {
            this.token = token;
        }

        @Override
        void write(JsonGenerator generator) throws IOException {
            // Writing the original token avoids BigDecimal scale limits and signed-zero loss.
            generator.writeRawValue(token);
        }
    }

    private static final class BooleanRescue extends RescueValue {
        private final boolean value;

        private BooleanRescue(boolean value) {
            this.value = value;
        }

        @Override
        void write(JsonGenerator generator) throws IOException {
            generator.writeBoolean(value);
        }
    }

    private static final class NullRescue extends RescueValue {
        private static final NullRescue INSTANCE = new NullRescue();

        @Override
        void write(JsonGenerator generator) throws IOException {
            generator.writeNull();
        }
    }

    static ObjectRescue objectRescue() {
        return new ObjectRescue();
    }

    /** Captures and consumes only the current value, preserving the spelling of number tokens. */
    static RescueValue capture(JsonParser parser) throws IOException {
        JsonToken token = parser.getCurrentToken();
        if (token == JsonToken.START_OBJECT) {
            ObjectRescue object = new ObjectRescue();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String name = parser.getCurrentName();
                parser.nextToken();
                object.add(name, capture(parser));
            }
            return object;
        }
        if (token == JsonToken.START_ARRAY) {
            ArrayRescue array = new ArrayRescue();
            int index = 0;
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                array.set(index++, capture(parser));
            }
            return array;
        }
        if (token == JsonToken.VALUE_STRING) {
            return new StringRescue(parser.getText());
        }
        if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
            return new NumberRescue(parser.getText());
        }
        if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
            return new BooleanRescue(token == JsonToken.VALUE_TRUE);
        }
        if (token == JsonToken.VALUE_NULL) {
            return NullRescue.INSTANCE;
        }
        throw new IOException("Unexpected JSON token " + token + ".");
    }

    static Converter create(DataType dataType) {
        try {
            KafkaTableSchema.validateJsonType(dataType);
        } catch (InvalidTableException e) {
            throw new KafkaTopicSchemaException(e.getMessage());
        }
        return createValidated(dataType);
    }

    private static Converter createValidated(DataType dataType) {
        NotNullConverter notNullConverter = createNotNull(dataType);
        return new Converter() {
            @Nullable
            @Override
            public Object convert(JsonParser parser, String path, @Nullable RescueSink rescue)
                    throws IOException {
                if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
                    return missingOrNull(dataType, path);
                }
                try {
                    return notNullConverter.convert(parser, path, rescue);
                } catch (KafkaRecordEncodingException e) {
                    throw e;
                } catch (ConversionException | NumberFormatException e) {
                    if (rescue != null && dataType.isNullable()) {
                        rescue.accept(capture(parser));
                        return null;
                    }
                    throw e instanceof ConversionException
                            ? ((ConversionException) e).recordException()
                            : invalid(path, dataType, "value cannot be converted", e)
                                    .recordException();
                } catch (RuntimeException e) {
                    throw invalid(path, dataType, "value cannot be converted", e).recordException();
                }
            }

            @Nullable
            @Override
            public Object convertMissing(String path) {
                return missingOrNull(dataType, path);
            }
        };
    }

    private static Object missingOrNull(DataType dataType, String path) {
        if (!dataType.isNullable()) {
            throw invalid(path, dataType, "field is missing or null").recordException();
        }
        return null;
    }

    private static NotNullConverter createNotNull(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return booleanConverter();
            case TINYINT:
                return tinyIntConverter();
            case SMALLINT:
                return smallIntConverter();
            case INTEGER:
                return intConverter(dataType);
            case BIGINT:
                return bigIntConverter(dataType);
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

    private static NotNullConverter booleanConverter() {
        return (parser, path, rescue) -> {
            JsonToken token = parser.getCurrentToken();
            if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
                return token == JsonToken.VALUE_TRUE;
            }
            if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                parser.skipChildren();
                return false;
            }
            return Boolean.parseBoolean(parser.getText().trim());
        };
    }

    private static NotNullConverter tinyIntConverter() {
        return (parser, path, rescue) -> Byte.parseByte(parser.getText().trim());
    }

    private static NotNullConverter smallIntConverter() {
        return (parser, path, rescue) -> Short.parseShort(parser.getText().trim());
    }

    private static NotNullConverter intConverter(DataType dataType) {
        return (parser, path, rescue) -> {
            if (!isNumber(parser.getCurrentToken())) {
                return Integer.parseInt(parser.getText().trim());
            }
            BigDecimal value = new BigDecimal(parser.getText());
            require(
                    value.compareTo(MIN_INT) >= 0 && value.compareTo(MAX_INT) <= 0,
                    path,
                    dataType,
                    "value cannot be converted");
            return value.intValue();
        };
    }

    private static NotNullConverter bigIntConverter(DataType dataType) {
        return (parser, path, rescue) -> {
            if (!isNumber(parser.getCurrentToken())) {
                return Long.parseLong(parser.getText().trim());
            }
            BigDecimal value = new BigDecimal(parser.getText());
            require(
                    value.compareTo(MIN_LONG) >= 0 && value.compareTo(MAX_LONG) <= 0,
                    path,
                    dataType,
                    "value cannot be converted");
            return value.longValue();
        };
    }

    private static NotNullConverter floatConverter(DataType dataType) {
        return (parser, path, rescue) -> {
            float value = Float.parseFloat(parser.getText().trim());
            require(Float.isFinite(value), path, dataType, "non-finite or overflowing number");
            return value;
        };
    }

    private static NotNullConverter doubleConverter(DataType dataType) {
        return (parser, path, rescue) -> {
            double value = Double.parseDouble(parser.getText().trim());
            require(Double.isFinite(value), path, dataType, "non-finite or overflowing number");
            return value;
        };
    }

    private static NotNullConverter decimalConverter(DecimalType dataType) {
        return (parser, path, rescue) -> {
            BigDecimal value = new BigDecimal(parser.getText());
            if (value.signum() == 0) {
                return Decimal.zero(dataType.getPrecision(), dataType.getScale());
            }
            long scaledPrecision = (long) value.precision() - value.scale() + dataType.getScale();
            require(
                    scaledPrecision <= dataType.getPrecision(),
                    path,
                    dataType,
                    "decimal precision exceeds target");
            if (scaledPrecision < 0) {
                return Decimal.zero(dataType.getPrecision(), dataType.getScale());
            }
            Decimal decimal =
                    Decimal.fromBigDecimal(value, dataType.getPrecision(), dataType.getScale());
            if (decimal == null) {
                throw invalid(path, dataType, "decimal precision exceeds target");
            }
            return decimal;
        };
    }

    private static NotNullConverter stringConverter() {
        return (parser, path, rescue) -> {
            JsonToken token = parser.getCurrentToken();
            if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                return BinaryString.fromString(capture(parser).toJson(OBJECT_MAPPER));
            }
            return BinaryString.fromString(parser.getText());
        };
    }

    private static NotNullConverter binaryConverter(BinaryType dataType) {
        return (parser, path, rescue) -> {
            byte[] value = decodeBase64(parser, path, dataType);
            return value.length == dataType.getLength()
                    ? value
                    : Arrays.copyOf(value, dataType.getLength());
        };
    }

    private static NotNullConverter bytesConverter(DataType dataType) {
        return (parser, path, rescue) -> decodeBase64(parser, path, dataType);
    }

    private static NotNullConverter dateConverter(DataType dataType) {
        return (parser, path, rescue) -> {
            String value = textualValue(parser, path, dataType);
            try {
                return Math.toIntExact(LocalDate.parse(value).toEpochDay());
            } catch (DateTimeException | ArithmeticException e) {
                throw invalid(path, dataType, "invalid ISO-8601 date", e);
            }
        };
    }

    private static NotNullConverter timeConverter(TimeType dataType) {
        int nanosPerUnit = nanosPerUnit(Math.min(dataType.getPrecision(), 3));
        return (parser, path, rescue) -> {
            String value = textualValue(parser, path, dataType);
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

    private static NotNullConverter timestampConverter(TimestampType dataType) {
        int nanosPerUnit = nanosPerUnit(dataType.getPrecision());
        return (parser, path, rescue) -> {
            String value = textualValue(parser, path, dataType).replace(' ', 'T');
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

    private static NotNullConverter timestampLtzConverter(LocalZonedTimestampType dataType) {
        int nanosPerUnit = nanosPerUnit(dataType.getPrecision());
        ZoneId localZone = ZoneId.systemDefault();
        return (parser, path, rescue) -> {
            String value = textualValue(parser, path, dataType).replace(' ', 'T');
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

    private static NotNullConverter arrayConverter(ArrayType dataType) {
        Converter elementConverter = createValidated(dataType.getElementType());
        return (parser, path, rescue) -> {
            requireToken(parser, JsonToken.START_ARRAY, path, dataType, "expected a JSON array");
            ArrayRescue rescued = rescue == null ? null : new ArrayRescue();
            List<Object> elements = new ArrayList<>();
            int index = 0;
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                final int elementIndex = index;
                elements.add(
                        elementConverter.convert(
                                parser,
                                JsonPath.index(path, elementIndex),
                                rescued == null
                                        ? null
                                        : value -> rescued.set(elementIndex, value)));
                index++;
            }
            if (rescued != null && !rescued.elements.isEmpty()) {
                rescued.ensureSize(index);
                rescue.accept(rescued);
            }
            return new GenericArray(elements.toArray(new Object[0]));
        };
    }

    private static NotNullConverter mapConverter(MapType dataType) {
        Converter valueConverter = createValidated(dataType.getValueType());
        return (parser, path, rescue) -> {
            requireToken(parser, JsonToken.START_OBJECT, path, dataType, "expected a JSON object");
            ObjectRescue rescued = rescue == null ? null : new ObjectRescue();
            Map<BinaryString, Object> values = new LinkedHashMap<>();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String name = parser.getCurrentName();
                parser.nextToken();
                values.put(
                        BinaryString.fromString(name),
                        valueConverter.convert(
                                parser,
                                JsonPath.field(path, name),
                                rescued == null ? null : value -> rescued.add(name, value)));
            }
            if (rescued != null && !rescued.isEmpty()) {
                rescue.accept(rescued);
            }
            return new GenericMap(values);
        };
    }

    private static NotNullConverter rowConverter(RowType dataType) {
        List<DataField> fields = dataType.getFields();
        String[] fieldNames = new String[fields.size()];
        Map<String, Integer> fieldPositions = new HashMap<>();
        Converter[] fieldConverters = new Converter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            fieldNames[i] = field.getName();
            fieldPositions.put(field.getName(), i);
            fieldConverters[i] = createValidated(field.getType());
        }
        return (parser, path, rescue) -> {
            requireToken(parser, JsonToken.START_OBJECT, path, dataType, "expected a JSON object");
            ObjectRescue rescued = rescue == null ? null : new ObjectRescue();
            GenericRow row = new GenericRow(fieldNames.length);
            boolean[] present = new boolean[fieldNames.length];
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String name = parser.getCurrentName();
                parser.nextToken();
                Integer position = fieldPositions.get(name);
                if (position == null) {
                    if (rescued == null) {
                        parser.skipChildren();
                    } else {
                        rescued.add(name, capture(parser));
                    }
                    continue;
                }
                row.setField(
                        position,
                        fieldConverters[position].convert(
                                parser,
                                JsonPath.field(path, name),
                                rescued == null ? null : value -> rescued.add(name, value)));
                present[position] = true;
            }
            for (int i = 0; i < fieldNames.length; i++) {
                if (!present[i]) {
                    row.setField(
                            i,
                            fieldConverters[i].convertMissing(JsonPath.field(path, fieldNames[i])));
                }
            }
            if (rescued != null && !rescued.isEmpty()) {
                rescue.accept(rescued);
            }
            return row;
        };
    }

    private static boolean isNumber(JsonToken token) {
        return token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT;
    }

    private static String textualValue(JsonParser parser, String path, DataType dataType)
            throws IOException {
        requireToken(parser, JsonToken.VALUE_STRING, path, dataType, "expected a JSON string");
        return parser.getText();
    }

    private static byte[] decodeBase64(JsonParser parser, String path, DataType dataType) {
        requireToken(
                parser,
                JsonToken.VALUE_STRING,
                path,
                dataType,
                "expected a Base64 string or binary value");
        try {
            return parser.getBinaryValue();
        } catch (IOException | IllegalArgumentException e) {
            throw invalid(path, dataType, "invalid Base64 string", e);
        }
    }

    private static void requireToken(
            JsonParser parser, JsonToken token, String path, DataType dataType, String reason) {
        require(parser.getCurrentToken() == token, path, dataType, reason);
    }

    private static int nanosPerUnit(int precision) {
        int unit = 1;
        for (int i = precision; i < 9; i++) {
            unit *= 10;
        }
        return unit;
    }

    private static long timestampMillis(
            Instant timestamp, int precision, String path, DataType dataType) {
        try {
            long millis = timestamp.toEpochMilli();
            int nanos = timestamp.getNano() % 1_000_000;
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

    @FunctionalInterface
    private interface NotNullConverter {
        Object convert(JsonParser parser, String path, @Nullable RescueSink rescue)
                throws IOException;
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
