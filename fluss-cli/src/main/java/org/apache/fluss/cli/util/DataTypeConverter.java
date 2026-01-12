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

package org.apache.fluss.cli.util;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Base64;

/** Utility class for converting between SQL string values and Fluss internal data types. */
public class DataTypeConverter {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME;
    // For parsing: accept both space and 'T' separator
    private static final DateTimeFormatter TIMESTAMP_PARSER =
            DateTimeFormatter.ofPattern("[yyyy-MM-dd HH:mm:ss][yyyy-MM-dd'T'HH:mm:ss]");
    // For display: use only space separator
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Convert a SQL string value to Fluss internal format for setting in GenericRow.
     *
     * @param value the string value from SQL
     * @param dataType the target Fluss data type
     * @return the converted value in Fluss internal format
     */
    public static Object convertFromString(String value, DataType dataType) {
        if (value == null || value.equalsIgnoreCase("NULL")) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        try {
            switch (typeRoot) {
                case BOOLEAN:
                    return Boolean.parseBoolean(value);
                case TINYINT:
                    return Byte.parseByte(value);
                case SMALLINT:
                    return Short.parseShort(value);
                case INTEGER:
                    return Integer.parseInt(value);
                case BIGINT:
                    return Long.parseLong(value);
                case FLOAT:
                    return Float.parseFloat(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case DECIMAL:
                    BigDecimal bd = new BigDecimal(value);
                    return Decimal.fromBigDecimal(
                            bd,
                            DataTypeChecks.getPrecision(dataType),
                            DataTypeChecks.getScale(dataType));
                case CHAR:
                case STRING:
                    String str = value;
                    if (str.startsWith("'") && str.endsWith("'")) {
                        str = str.substring(1, str.length() - 1);
                    }
                    return BinaryString.fromString(str);
                case BINARY:
                case BYTES:
                    if (value.startsWith("0x") || value.startsWith("0X")) {
                        return hexStringToBytes(value.substring(2));
                    } else {
                        return Base64.getDecoder().decode(value);
                    }
                case DATE:
                    LocalDate date = LocalDate.parse(value, DATE_FORMATTER);
                    return (int) date.toEpochDay();
                case TIME_WITHOUT_TIME_ZONE:
                    LocalTime time = LocalTime.parse(value, TIME_FORMATTER);
                    return (int) (time.toNanoOfDay() / 1_000_000);
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    LocalDateTime ldt = LocalDateTime.parse(value, TIMESTAMP_PARSER);
                    return TimestampNtz.fromLocalDateTime(ldt);
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    Instant instant = Instant.parse(value);
                    return TimestampLtz.fromInstant(instant);
                case ARRAY:
                    return ComplexTypeLiteralParser.parseArray(value, (ArrayType) dataType);
                case MAP:
                    return ComplexTypeLiteralParser.parseMap(value, (MapType) dataType);
                case ROW:
                    return ComplexTypeLiteralParser.parseRow(value, (RowType) dataType);
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + typeRoot);
            }
        } catch (DateTimeParseException | NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Failed to parse value '"
                            + value
                            + "' for type "
                            + typeRoot
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }

    /**
     * Convert a Fluss internal value to a human-readable string for display.
     *
     * @param value the internal value
     * @param dataType the Fluss data type
     * @return the string representation
     */
    public static String convertToString(Object value, DataType dataType) {
        if (value == null) {
            return "NULL";
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return value.toString();
            case DECIMAL:
                Decimal decimal = (Decimal) value;
                return decimal.toBigDecimal().toString();
            case CHAR:
            case STRING:
                return ((BinaryString) value).toString();
            case BINARY:
            case BYTES:
                byte[] bytes = (byte[]) value;
                return "0x" + bytesToHexString(bytes);
            case DATE:
                int days = (Integer) value;
                LocalDate date = LocalDate.ofEpochDay(days);
                return date.format(DATE_FORMATTER);
            case TIME_WITHOUT_TIME_ZONE:
                int millis = (Integer) value;
                LocalTime time = LocalTime.ofNanoOfDay(millis * 1_000_000L);
                return time.format(TIME_FORMATTER);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampNtz tsNtz = (TimestampNtz) value;
                return tsNtz.toLocalDateTime().format(TIMESTAMP_FORMATTER);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampLtz tsLtz = (TimestampLtz) value;
                return tsLtz.toInstant().toString();
            case ARRAY:
                return formatArray((InternalArray) value, (ArrayType) dataType);
            case MAP:
                return formatMap((InternalMap) value, (MapType) dataType);
            case ROW:
                return formatRow((InternalRow) value, (RowType) dataType);
            default:
                return value.toString();
        }
    }

    /**
     * Get a value from InternalRow and convert it to string for display.
     *
     * @param row the internal row
     * @param fieldIndex the field index
     * @param dataType the data type
     * @return the string representation
     */
    public static String getFieldAsString(InternalRow row, int fieldIndex, DataType dataType) {
        if (row.isNullAt(fieldIndex)) {
            return "NULL";
        }

        Object value;
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BOOLEAN:
                value = row.getBoolean(fieldIndex);
                break;
            case TINYINT:
                value = row.getByte(fieldIndex);
                break;
            case SMALLINT:
                value = row.getShort(fieldIndex);
                break;
            case INTEGER:
                value = row.getInt(fieldIndex);
                break;
            case BIGINT:
                value = row.getLong(fieldIndex);
                break;
            case FLOAT:
                value = row.getFloat(fieldIndex);
                break;
            case DOUBLE:
                value = row.getDouble(fieldIndex);
                break;
            case DECIMAL:
                value =
                        row.getDecimal(
                                fieldIndex,
                                DataTypeChecks.getPrecision(dataType),
                                DataTypeChecks.getScale(dataType));
                break;
            case CHAR:
            case STRING:
                value = row.getString(fieldIndex);
                break;
            case BINARY:
            case BYTES:
                value = row.getBytes(fieldIndex);
                break;
            case DATE:
                value = row.getInt(fieldIndex);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                value = row.getInt(fieldIndex);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                value = row.getTimestampNtz(fieldIndex, DataTypeChecks.getPrecision(dataType));
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                value = row.getTimestampLtz(fieldIndex, DataTypeChecks.getPrecision(dataType));
                break;
            case ARRAY:
                value = row.getArray(fieldIndex);
                break;
            case MAP:
                value = row.getMap(fieldIndex);
                break;
            case ROW:
                value = row.getRow(fieldIndex, ((RowType) dataType).getFieldCount());
                break;
            default:
                return "<unsupported type: " + typeRoot + ">";
        }

        return convertToString(value, dataType);
    }

    public static Object getFieldValue(InternalRow row, int fieldIndex, DataType dataType) {
        if (row.isNullAt(fieldIndex)) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BOOLEAN:
                return row.getBoolean(fieldIndex);
            case TINYINT:
                return row.getByte(fieldIndex);
            case SMALLINT:
                return row.getShort(fieldIndex);
            case INTEGER:
                return row.getInt(fieldIndex);
            case BIGINT:
                return row.getLong(fieldIndex);
            case FLOAT:
                return row.getFloat(fieldIndex);
            case DOUBLE:
                return row.getDouble(fieldIndex);
            case DECIMAL:
                return row.getDecimal(
                        fieldIndex,
                        DataTypeChecks.getPrecision(dataType),
                        DataTypeChecks.getScale(dataType));
            case CHAR:
            case STRING:
                return row.getString(fieldIndex);
            case BINARY:
            case BYTES:
                return row.getBytes(fieldIndex);
            case DATE:
                return row.getInt(fieldIndex);
            case TIME_WITHOUT_TIME_ZONE:
                return row.getInt(fieldIndex);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return row.getTimestampNtz(fieldIndex, DataTypeChecks.getPrecision(dataType));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return row.getTimestampLtz(fieldIndex, DataTypeChecks.getPrecision(dataType));
            case ARRAY:
                return row.getArray(fieldIndex);
            case MAP:
                return row.getMap(fieldIndex);
            case ROW:
                return row.getRow(fieldIndex, ((RowType) dataType).getFieldCount());
            default:
                throw new IllegalArgumentException("Unsupported data type: " + typeRoot);
        }
    }

    private static String formatArray(InternalArray array, ArrayType arrayType) {
        StringBuilder sb = new StringBuilder("ARRAY[");
        DataType elementType = arrayType.getElementType();
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(elementType);
        for (int i = 0; i < array.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Object element = array.isNullAt(i) ? null : elementGetter.getElementOrNull(array, i);
            sb.append(convertToString(element, elementType));
        }
        sb.append(']');
        return sb.toString();
    }

    private static String formatMap(InternalMap map, MapType mapType) {
        StringBuilder sb = new StringBuilder("MAP[");
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType);
        InternalArray.ElementGetter valueGetter = InternalArray.createElementGetter(valueType);
        for (int i = 0; i < map.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Object key = keyArray.isNullAt(i) ? null : keyGetter.getElementOrNull(keyArray, i);
            Object value =
                    valueArray.isNullAt(i) ? null : valueGetter.getElementOrNull(valueArray, i);
            sb.append(convertToString(key, keyType));
            sb.append(", ");
            sb.append(convertToString(value, valueType));
        }
        sb.append(']');
        return sb.toString();
    }

    private static String formatRow(InternalRow row, RowType rowType) {
        StringBuilder sb = new StringBuilder("ROW(");
        int fieldCount = rowType.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            DataType fieldType = rowType.getTypeAt(i);
            Object value =
                    row.isNullAt(i)
                            ? null
                            : InternalRow.createFieldGetter(fieldType, i).getFieldOrNull(row);
            sb.append(convertToString(value, fieldType));
        }
        sb.append(')');
        return sb.toString();
    }

    private static byte[] hexStringToBytes(String hex) {
        int len = hex.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException(
                    "Hex string must have even length, but got length " + len + ": " + hex);
        }
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            int digit1 = Character.digit(hex.charAt(i), 16);
            int digit2 = Character.digit(hex.charAt(i + 1), 16);
            if (digit1 == -1 || digit2 == -1) {
                throw new IllegalArgumentException("Invalid hex string: " + hex);
            }
            data[i / 2] = (byte) ((digit1 << 4) + digit2);
        }
        return data;
    }

    private static String bytesToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
