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

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinarySection;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.MurmurHashUtils;

import java.io.Serializable;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.types.DataTypeChecks.getScale;

/** Value comparison independent of array and nested row encoding. */
final class ArrayElementComparator implements Serializable {

    private static final long serialVersionUID = 1L;

    static final int NULL_HASH = 1;

    private final DataType type;
    private final ArrayElementComparator[] children;

    ArrayElementComparator(DataType type) {
        this.type = type;
        switch (type.getTypeRoot()) {
            case ARRAY:
                children =
                        new ArrayElementComparator[] {
                            new ArrayElementComparator(((ArrayType) type).getElementType())
                        };
                break;
            case MAP:
                MapType map = (MapType) type;
                children =
                        new ArrayElementComparator[] {
                            new ArrayElementComparator(map.getKeyType()),
                            new ArrayElementComparator(map.getValueType())
                        };
                break;
            case ROW:
                RowType row = (RowType) type;
                children = new ArrayElementComparator[row.getFieldCount()];
                for (int i = 0; i < children.length; i++) {
                    children[i] = new ArrayElementComparator(row.getFieldTypes().get(i));
                }
                break;
            default:
                children = new ArrayElementComparator[0];
        }
    }

    boolean supportsLongKey() {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    long longKey(DataGetters array, int pos) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return array.getBoolean(pos) ? 1 : 0;
            case TINYINT:
                return array.getByte(pos);
            case SMALLINT:
                return array.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return array.getInt(pos);
            case BIGINT:
                return array.getLong(pos);
            case FLOAT:
                return Float.floatToIntBits(array.getFloat(pos));
            case DOUBLE:
                return Double.doubleToLongBits(array.getDouble(pos));
            default:
                throw new IllegalArgumentException("Not a primitive key: " + type);
        }
    }

    int hash(DataGetters array, int pos) {
        if (array.isNullAt(pos)) {
            return NULL_HASH;
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return hashLong(longKey(array, pos));
            case CHAR:
            case STRING:
            case BINARY:
            case BYTES:
                if (array instanceof BinaryArray) {
                    BinaryArray binary = (BinaryArray) array;
                    long region = byteRegion(binary, pos);
                    return BinarySegmentUtils.hash(
                            binary.getSegments(), (int) (region >>> 32), (int) region);
                }
                Object value = value(array, pos);
                return value instanceof byte[]
                        ? MurmurHashUtils.hashBytes((byte[]) value)
                        : value.hashCode();
            case ARRAY:
                InternalArray nested = array.getArray(pos);
                int hash = 1;
                for (int i = 0; i < nested.size(); i++) {
                    hash = 31 * hash + children[0].hash(nested, i);
                }
                return hash;
            case ROW:
                InternalRow row = array.getRow(pos, children.length);
                int rowHash = 1;
                for (int i = 0; i < children.length; i++) {
                    rowHash = 31 * rowHash + children[i].hash(row, i);
                }
                return rowHash;
            case MAP:
                InternalMap map = array.getMap(pos);
                InternalArray keys = map.keyArray();
                InternalArray values = map.valueArray();
                int mapHash = map.size();
                for (int i = 0; i < map.size(); i++) {
                    long entry =
                            ((long) children[0].hash(keys, i) << 32)
                                    | (children[1].hash(values, i) & 0xffffffffL);
                    mapHash += hashLong(entry);
                }
                return MurmurHashUtils.fmix(mapHash);
            default:
                return value(array, pos).hashCode();
        }
    }

    /**
     * Mixes both halves before folding, preserving hashLong(0) == 0 for primitive null handling.
     */
    static int hashLong(long value) {
        // MurmurHash3's 64-bit finalizer.
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53L;
        value ^= value >>> 33;
        return (int) (value ^ (value >>> 32));
    }

    boolean equal(DataGetters left, int l, DataGetters right, int r) {
        if (left.isNullAt(l) || right.isNullAt(r)) {
            return left.isNullAt(l) && right.isNullAt(r);
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return left.getBoolean(l) == right.getBoolean(r);
            case TINYINT:
                return left.getByte(l) == right.getByte(r);
            case SMALLINT:
                return left.getShort(l) == right.getShort(r);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return left.getInt(l) == right.getInt(r);
            case BIGINT:
                return left.getLong(l) == right.getLong(r);
            case FLOAT:
                return Float.floatToIntBits(left.getFloat(l))
                        == Float.floatToIntBits(right.getFloat(r));
            case DOUBLE:
                return Double.doubleToLongBits(left.getDouble(l))
                        == Double.doubleToLongBits(right.getDouble(r));
            case CHAR:
            case STRING:
            case BINARY:
            case BYTES:
                if (left instanceof BinaryArray && right instanceof BinaryArray) {
                    BinaryArray a = (BinaryArray) left;
                    BinaryArray b = (BinaryArray) right;
                    long ar = byteRegion(a, l);
                    long br = byteRegion(b, r);
                    return (int) ar == (int) br
                            && BinarySegmentUtils.equals(
                                    a.getSegments(),
                                    (int) (ar >>> 32),
                                    b.getSegments(),
                                    (int) (br >>> 32),
                                    (int) ar);
                }
                Object a = value(left, l);
                Object b = value(right, r);
                return a instanceof byte[] ? Arrays.equals((byte[]) a, (byte[]) b) : a.equals(b);
            case ARRAY:
                InternalArray la = left.getArray(l);
                InternalArray ra = right.getArray(r);
                if (la.size() != ra.size()) {
                    return false;
                }
                for (int i = 0; i < la.size(); i++) {
                    if (!children[0].equal(la, i, ra, i)) {
                        return false;
                    }
                }
                return true;
            case ROW:
                InternalRow lr = left.getRow(l, children.length);
                InternalRow rr = right.getRow(r, children.length);
                for (int i = 0; i < children.length; i++) {
                    if (!children[i].equal(lr, i, rr, i)) {
                        return false;
                    }
                }
                return true;
            case MAP:
                InternalMap lm = left.getMap(l);
                InternalMap rm = right.getMap(r);
                if (lm.size() != rm.size()) {
                    return false;
                }
                InternalArray lk = lm.keyArray();
                InternalArray rk = rm.keyArray();
                InternalArray lv = lm.valueArray();
                InternalArray rv = rm.valueArray();
                for (int i = 0; i < lm.size(); i++) {
                    int j = i;
                    if (!children[0].equal(lk, i, rk, j)) {
                        j = 0;
                        while (j < rm.size() && !children[0].equal(lk, i, rk, j)) {
                            j++;
                        }
                    }
                    if (j == rm.size() || !children[1].equal(lv, i, rv, j)) {
                        return false;
                    }
                }
                return true;
            default:
                return value(left, l).equals(value(right, r));
        }
    }

    private Object value(DataGetters array, int pos) {
        switch (type.getTypeRoot()) {
            case CHAR:
                return array.getChar(pos, getLength(type));
            case STRING:
                return array.getString(pos);
            case BINARY:
                return array.getBinary(pos, getLength(type));
            case BYTES:
                return array.getBytes(pos);
            case DECIMAL:
                return array.getDecimal(pos, getPrecision(type), getScale(type));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return array.getTimestampNtz(pos, getPrecision(type));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return array.getTimestampLtz(pos, getPrecision(type));
            default:
                throw new IllegalArgumentException("Unsupported element type: " + type);
        }
    }

    /** Packs the absolute content offset and length, excluding slot metadata and padding. */
    private static long byteRegion(BinaryArray array, int pos) {
        long slot = array.getLong(pos);
        int offset;
        int length;
        if ((slot & BinarySection.HIGHEST_FIRST_BIT) != 0) {
            offset = array.getOffset() + BinaryArray.calculateHeaderInBytes(array.size()) + pos * 8;
            if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
                offset++;
            }
            length = (int) ((slot >>> 56) & 0x7f);
        } else {
            offset = array.getOffset() + (int) (slot >>> 32);
            length = (int) slot;
        }
        return ((long) offset << 32) | (length & 0xffffffffL);
    }
}
