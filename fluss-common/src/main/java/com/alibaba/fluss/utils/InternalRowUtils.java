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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.DataGetters;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimestampType;

import static com.alibaba.fluss.types.DataTypeChecks.getLength;

/** InternalRowUtils. */
public class InternalRowUtils {

    public static InternalRow copyInternalRow(InternalRow row, RowType rowType) {
        if (row instanceof BinaryRow) {
            // TODO: Implement copy() method for BinaryRow in subsequent PRs
            throw new UnsupportedOperationException("BinaryRow copy not yet implemented");
        } else {
            GenericRow ret = new GenericRow(row.getFieldCount());

            for (int i = 0; i < row.getFieldCount(); ++i) {
                DataType fieldType = rowType.getTypeAt(i);
                ret.setField(i, copy(get((DataGetters) row, i, fieldType), fieldType));
            }

            return ret;
        }
    }

    public static Object copy(Object o, DataType type) {
        if (o instanceof BinaryString) {
            return ((BinaryString) o).copy();
        } else if (o instanceof InternalRow) {
            return copyInternalRow((InternalRow) o, (RowType) type);
        } else if (o instanceof Decimal) {
            return ((Decimal) o).copy();
        }
        // Array and Map support will be added in subsequent PRs
        return o;
    }

    public static Object get(DataGetters dataGetters, int pos, DataType fieldType) {
        if (dataGetters.isNullAt(pos)) {
            return null;
        }
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return dataGetters.getBoolean(pos);
            case TINYINT:
                return dataGetters.getByte(pos);
            case SMALLINT:
                return dataGetters.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return dataGetters.getInt(pos);
            case BIGINT:
                return dataGetters.getLong(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                return dataGetters.getTimestampNtz(pos, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) fieldType;
                return dataGetters.getTimestampLtz(pos, lzTs.getPrecision());
            case FLOAT:
                return dataGetters.getFloat(pos);
            case DOUBLE:
                return dataGetters.getDouble(pos);
            case CHAR:
            case STRING:
                return dataGetters.getString(pos);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return dataGetters.getDecimal(
                        pos, decimalType.getPrecision(), decimalType.getScale());
            case ARRAY:
                // Array support will be added in subsequent PRs
                throw new UnsupportedOperationException("Array support not yet implemented");
            case MAP:
                // Map support will be added in subsequent PRs
                throw new UnsupportedOperationException("Map support not yet implemented");
            case ROW:
                return dataGetters.getRow(pos, ((RowType) fieldType).getFieldCount());
            case BINARY:
                final int binaryLength = getLength(fieldType);
                return dataGetters.getBinary(pos, binaryLength);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }
}
