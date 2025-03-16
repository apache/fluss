/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.TimestampType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/** Type related helper functions. */
public class TypeUtils {
    public static Object castFromString(String s, DataType type) {
        BinaryString str = BinaryString.fromString(s);
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                return str;
            case BOOLEAN:
                return BinaryStringUtils.toBoolean(str);
            case BINARY:
            case BYTES:
                return s.getBytes(StandardCharsets.UTF_8);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return Decimal.fromBigDecimal(
                        new BigDecimal(s), decimalType.getPrecision(), decimalType.getScale());
            case TINYINT:
                return Byte.valueOf(s);
            case SMALLINT:
                return Short.valueOf(s);
            case INTEGER:
                return Integer.valueOf(s);
            case BIGINT:
                return Long.valueOf(s);
            case FLOAT:
                return Float.parseFloat(s);
            case DOUBLE:
                return Double.valueOf(s);
            case DATE:
                return BinaryStringUtils.toDate(str);
            case TIME_WITHOUT_TIME_ZONE:
                return BinaryStringUtils.toTime(str);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                return BinaryStringUtils.toTimestampNtz(str, timestampType.getPrecision());
            default:
                throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }
}
