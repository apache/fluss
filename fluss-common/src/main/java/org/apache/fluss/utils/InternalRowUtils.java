/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.utils;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

/** Utility class for {@link org.apache.fluss.row.InternalRow} related operations. */
public class InternalRowUtils {

    /**
     * Compares two objects based on their data type.
     *
     * @param x the first object
     * @param y the second object
     * @param type the data type
     * @return a negative integer, zero, or a positive integer as x is less than, equal to, or
     *     greater than y
     */
    public static int compare(Object x, Object y, DataType type) {
        switch (type.getTypeRoot()) {
            case ARRAY:
                return compareArray((InternalArray) x, (InternalArray) y, (ArrayType) type);
            case ROW:
                return compareRow((InternalRow) x, (InternalRow) y, (RowType) type);
            case MAP:
                return compareMap((InternalMap) x, (InternalMap) y, (MapType) type);
            default:
                return compare(x, y, type.getTypeRoot());
        }
    }

    /**
     * Compares two objects based on their data type.
     *
     * @param x the first object
     * @param y the second object
     * @param type the data type root
     * @return a negative integer, zero, or a positive integer as x is less than, equal to, or
     *     greater than y
     */
    public static int compare(Object x, Object y, DataTypeRoot type) {
        int ret;
        switch (type) {
            case DECIMAL:
                Decimal xDD = (Decimal) x;
                Decimal yDD = (Decimal) y;
                ret = xDD.compareTo(yDD);
                break;
            case TINYINT:
                ret = Byte.compare((byte) x, (byte) y);
                break;
            case SMALLINT:
                ret = Short.compare((short) x, (short) y);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                ret = Integer.compare((int) x, (int) y);
                break;
            case BIGINT:
                ret = Long.compare((long) x, (long) y);
                break;
            case FLOAT:
                ret = Float.compare((float) x, (float) y);
                break;
            case DOUBLE:
                ret = Double.compare((double) x, (double) y);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampNtz xNtz = (TimestampNtz) x;
                TimestampNtz yNtz = (TimestampNtz) y;
                ret = xNtz.compareTo(yNtz);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampLtz xLtz = (TimestampLtz) x;
                TimestampLtz yLtz = (TimestampLtz) y;
                ret = xLtz.compareTo(yLtz);
                break;
            case BINARY:
            case BYTES:
                ret = byteArrayCompare((byte[]) x, (byte[]) y);
                break;
            case STRING:
            case CHAR:
                ret = ((BinaryString) x).compareTo((BinaryString) y);
                break;
            default:
                throw new IllegalArgumentException("Incomparable type: " + type);
        }
        return ret;
    }

    private static int compareArray(InternalArray a1, InternalArray a2, ArrayType type) {
        int size1 = a1.size();
        int size2 = a2.size();
        int size = Math.min(size1, size2);
        InternalArray.ElementGetter getter =
                InternalArray.createElementGetter(type.getElementType());

        for (int i = 0; i < size; i++) {
            Object o1 = getter.getElementOrNull(a1, i);
            Object o2 = getter.getElementOrNull(a2, i);

            if (o1 == null && o2 == null) {
                continue;
            }
            if (o1 == null) {
                return -1;
            }
            if (o2 == null) {
                return 1;
            }

            int cmp = compare(o1, o2, type.getElementType());
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(size1, size2);
    }

    private static int compareRow(InternalRow r1, InternalRow r2, RowType type) {
        int count = type.getFieldCount();
        for (int i = 0; i < count; i++) {
            InternalRow.FieldGetter getter = InternalRow.createFieldGetter(type.getTypeAt(i), i);
            Object o1 = getter.getFieldOrNull(r1);
            Object o2 = getter.getFieldOrNull(r2);

            if (o1 == null && o2 == null) {
                continue;
            }
            if (o1 == null) {
                return -1;
            }
            if (o2 == null) {
                return 1;
            }

            int cmp = compare(o1, o2, type.getTypeAt(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private static int compareMap(InternalMap m1, InternalMap m2, MapType type) {
        throw new IllegalArgumentException("Map type is not comparable: " + type);
    }

    private static int byteArrayCompare(byte[] array1, byte[] array2) {
        for (int i = 0, j = 0; i < array1.length && j < array2.length; i++, j++) {
            int a = (array1[i] & 0xff);
            int b = (array2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return array1.length - array2.length;
    }
}
