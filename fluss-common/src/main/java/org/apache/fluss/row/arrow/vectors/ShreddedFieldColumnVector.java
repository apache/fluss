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

package org.apache.fluss.row.arrow.vectors;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.columnar.BooleanColumnVector;
import org.apache.fluss.row.columnar.ByteColumnVector;
import org.apache.fluss.row.columnar.BytesColumnVector;
import org.apache.fluss.row.columnar.ColumnVector;
import org.apache.fluss.row.columnar.DoubleColumnVector;
import org.apache.fluss.row.columnar.FloatColumnVector;
import org.apache.fluss.row.columnar.IntColumnVector;
import org.apache.fluss.row.columnar.LongColumnVector;
import org.apache.fluss.row.columnar.ShortColumnVector;
import org.apache.fluss.row.columnar.TimestampLtzColumnVector;
import org.apache.fluss.row.columnar.TimestampNtzColumnVector;
import org.apache.fluss.row.columnar.VariantColumnVector;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.variant.Variant;
import org.apache.fluss.types.variant.VariantUtil;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;

/**
 * A virtual {@link ColumnVector} that exposes a typed top-level Variant sub-field as a top-level
 * scalar column. This is what lets the user write {@code scan.project(["data:age::BIGINT"])} and
 * then read the value directly via {@code row.getLong(idx)}.
 *
 * <h3>Type matching semantics (lenient mode)</h3>
 *
 * <p>The cast is performed strictly: the user-declared {@code castType} must have the same {@link
 * DataTypeRoot} as the shredded type stored on the server for the fast path. When any of the
 * following holds, the row is reported as {@code null}:
 *
 * <ul>
 *   <li>the user-declared {@code castType} does not match the shredded type root (e.g. user asked
 *       for BIGINT but the field is shredded as STRING);
 *   <li>the entire Variant value at this row is null;
 *   <li>the field's {@code typed_value} Arrow slot is null at this row, regardless of whether the
 *       residual fallback {@code value} slot is set ("type mismatch" rows fall back to null).
 * </ul>
 *
 * <p>If the requested top-level field is not present in the per-batch shredding schema, the vector
 * falls back to the raw/residual Variant binary returned by the server and decodes the field on the
 * client side. This is the FIP-36 miss case: functionally correct but more expensive than the
 * typed_value fast path.
 */
@Internal
public final class ShreddedFieldColumnVector {

    private ShreddedFieldColumnVector() {}

    /**
     * Creates a virtual column vector that projects the given typed top-level sub-field out of the
     * parent Variant column. Shredded batches use a typed_value fast path. Unshredded or miss-case
     * batches fall back to decoding the raw/residual Variant value.
     */
    public static ColumnVector create(
            @Nullable VariantColumnVector parent, String fieldName, DataType castType) {
        if (parent == null) {
            return new AlwaysNullVector(castType);
        }
        if (parent instanceof ShreddedVariantColumnVector) {
            ShreddedVariantColumnVector shreddedParent = (ShreddedVariantColumnVector) parent;
            int fieldIndex = shreddedParent.shreddedFieldIndex(fieldName);
            if (fieldIndex < 0) {
                // Miss case: this batch did not shred the requested field. The server keeps
                // metadata/value in the projected Variant column, so decode it locally.
                return new VariantFallbackVector(parent, fieldName, castType);
            }
            boolean typeMatches =
                    shreddedParent.shreddedFieldType(fieldIndex).getTypeRoot()
                            == castType.getTypeRoot();
            if (!typeMatches) {
                return new AlwaysNullVector(castType);
            }
            ColumnVector fastVector = createTypedValueVector(shreddedParent, fieldIndex, castType);
            if (fastVector != null) {
                return fastVector;
            }
            return new AlwaysNullVector(castType);
        }
        return new VariantFallbackVector(parent, fieldName, castType);
    }

    private static ColumnVector createTypedValueVector(
            ShreddedVariantColumnVector parent, int fieldIndex, DataType castType) {
        switch (castType.getTypeRoot()) {
            case BOOLEAN:
                return new BooleanImpl(parent, fieldIndex);
            case TINYINT:
                return new ByteImpl(parent, fieldIndex);
            case SMALLINT:
                return new ShortImpl(parent, fieldIndex);
            case INTEGER:
            case DATE:
                return new IntImpl(parent, fieldIndex);
            case BIGINT:
                return new LongImpl(parent, fieldIndex);
            case FLOAT:
                return new FloatImpl(parent, fieldIndex);
            case DOUBLE:
                return new DoubleImpl(parent, fieldIndex);
            case CHAR:
            case STRING:
                return new StringImpl(parent, fieldIndex);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TimestampNtzImpl(parent, fieldIndex);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampLtzImpl(parent, fieldIndex);
            default:
                return null;
        }
    }

    // ------------------------------------------------------------------------
    // Base type
    // ------------------------------------------------------------------------

    private abstract static class Base implements ColumnVector {
        protected final ShreddedVariantColumnVector parent;
        protected final int fieldIndex;

        Base(ShreddedVariantColumnVector parent, int fieldIndex) {
            this.parent = parent;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public final boolean isNullAt(int i) {
            return parent.shreddedFieldTypedNull(fieldIndex, i);
        }
    }

    // ------------------------------------------------------------------------
    // Type-specific implementations
    // ------------------------------------------------------------------------

    private static final class BooleanImpl extends Base implements BooleanColumnVector {
        BooleanImpl(ShreddedVariantColumnVector parent, int fieldIndex) {
            super(parent, fieldIndex);
        }

        @Override
        public boolean getBoolean(int i) {
            Object o = parent.shreddedFieldTypedObject(fieldIndex, i);
            return o instanceof Boolean && (Boolean) o;
        }
    }

    private static final class ByteImpl extends Base implements ByteColumnVector {
        ByteImpl(ShreddedVariantColumnVector parent, int fieldIndex) {
            super(parent, fieldIndex);
        }

        @Override
        public byte getByte(int i) {
            Object o = parent.shreddedFieldTypedObject(fieldIndex, i);
            return o instanceof Number ? ((Number) o).byteValue() : (byte) 0;
        }
    }

    private static final class ShortImpl extends Base implements ShortColumnVector {
        ShortImpl(ShreddedVariantColumnVector parent, int fieldIndex) {
            super(parent, fieldIndex);
        }

        @Override
        public short getShort(int i) {
            Object o = parent.shreddedFieldTypedObject(fieldIndex, i);
            return o instanceof Number ? ((Number) o).shortValue() : (short) 0;
        }
    }

    private static final class IntImpl extends Base implements IntColumnVector {
        IntImpl(ShreddedVariantColumnVector parent, int fieldIndex) {
            super(parent, fieldIndex);
        }

        @Override
        public int getInt(int i) {
            Object o = parent.shreddedFieldTypedObject(fieldIndex, i);
            return o instanceof Number ? ((Number) o).intValue() : 0;
        }
    }

    private static final class LongImpl extends Base implements LongColumnVector {
        LongImpl(ShreddedVariantColumnVector parent, int fieldIndex) {
            super(parent, fieldIndex);
        }

        @Override
        public long getLong(int i) {
            Object o = parent.shreddedFieldTypedObject(fieldIndex, i);
            return o instanceof Number ? ((Number) o).longValue() : 0L;
        }
    }

    private static final class FloatImpl extends Base implements FloatColumnVector {
        FloatImpl(ShreddedVariantColumnVector parent, int fieldIndex) {
            super(parent, fieldIndex);
        }

        @Override
        public float getFloat(int i) {
            Object o = parent.shreddedFieldTypedObject(fieldIndex, i);
            return o instanceof Number ? ((Number) o).floatValue() : 0f;
        }
    }

    private static final class DoubleImpl extends Base implements DoubleColumnVector {
        DoubleImpl(ShreddedVariantColumnVector parent, int fieldIndex) {
            super(parent, fieldIndex);
        }

        @Override
        public double getDouble(int i) {
            Object o = parent.shreddedFieldTypedObject(fieldIndex, i);
            return o instanceof Number ? ((Number) o).doubleValue() : 0d;
        }
    }

    private static final class StringImpl extends Base implements BytesColumnVector {
        StringImpl(ShreddedVariantColumnVector parent, int fieldIndex) {
            super(parent, fieldIndex);
        }

        @Override
        public Bytes getBytes(int i) {
            Object o = parent.shreddedFieldTypedObject(fieldIndex, i);
            byte[] bytes;
            if (o instanceof String) {
                bytes = ((String) o).getBytes(StandardCharsets.UTF_8);
            } else if (o instanceof byte[]) {
                bytes = (byte[]) o;
            } else {
                bytes = new byte[0];
            }
            return new Bytes(bytes, 0, bytes.length);
        }
    }

    private static final class TimestampNtzImpl extends Base implements TimestampNtzColumnVector {
        TimestampNtzImpl(ShreddedVariantColumnVector parent, int fieldIndex) {
            super(parent, fieldIndex);
        }

        @Override
        public TimestampNtz getTimestampNtz(int i, int precision) {
            Object o = parent.shreddedFieldTypedObject(fieldIndex, i);
            long micros = o instanceof Number ? ((Number) o).longValue() : 0L;
            // Variant micro precision: convert micros to (millis, nanosOfMillis).
            long millis = Math.floorDiv(micros, 1000L);
            int nanosOfMillis = (int) Math.floorMod(micros, 1000L) * 1000;
            return TimestampNtz.fromMillis(millis, nanosOfMillis);
        }
    }

    private static final class TimestampLtzImpl extends Base implements TimestampLtzColumnVector {
        TimestampLtzImpl(ShreddedVariantColumnVector parent, int fieldIndex) {
            super(parent, fieldIndex);
        }

        @Override
        public TimestampLtz getTimestampLtz(int i, int precision) {
            Object o = parent.shreddedFieldTypedObject(fieldIndex, i);
            long micros = o instanceof Number ? ((Number) o).longValue() : 0L;
            long millis = Math.floorDiv(micros, 1000L);
            int nanosOfMillis = (int) Math.floorMod(micros, 1000L) * 1000;
            return TimestampLtz.fromEpochMillis(millis, nanosOfMillis);
        }
    }

    /**
     * Raw/residual fallback used when a batch did not shred the requested top-level field. This is
     * intentionally limited to top-level field names; nested path parsing belongs to the follow-up
     * FIP work that will introduce a path object shared by parser, RPC, server pruning, and client
     * residual decode.
     */
    private static final class VariantFallbackVector
            implements ColumnVector,
                    BooleanColumnVector,
                    ByteColumnVector,
                    ShortColumnVector,
                    IntColumnVector,
                    LongColumnVector,
                    FloatColumnVector,
                    DoubleColumnVector,
                    BytesColumnVector,
                    TimestampNtzColumnVector,
                    TimestampLtzColumnVector {

        private final VariantColumnVector parent;
        private final String fieldName;
        private final DataType castType;

        VariantFallbackVector(VariantColumnVector parent, String fieldName, DataType castType) {
            this.parent = parent;
            this.fieldName = fieldName;
            this.castType = castType;
        }

        @Override
        public boolean isNullAt(int i) {
            return fieldAt(i) == null;
        }

        @Override
        public boolean getBoolean(int i) {
            Variant field = fieldAt(i);
            return field != null && field.getBoolean();
        }

        @Override
        public byte getByte(int i) {
            Variant field = fieldAt(i);
            return field != null ? field.getByte() : (byte) 0;
        }

        @Override
        public short getShort(int i) {
            Variant field = fieldAt(i);
            if (field == null) {
                return (short) 0;
            }
            int typeId = VariantUtil.primitiveTypeId(field.value(), 0);
            return typeId == VariantUtil.PRIMITIVE_TYPE_INT8 ? field.getByte() : field.getShort();
        }

        @Override
        public int getInt(int i) {
            Variant field = fieldAt(i);
            if (field == null) {
                return 0;
            }
            if (castType.getTypeRoot() == DataTypeRoot.DATE) {
                return field.getDate();
            }
            int typeId = VariantUtil.primitiveTypeId(field.value(), 0);
            if (typeId == VariantUtil.PRIMITIVE_TYPE_INT8) {
                return field.getByte();
            } else if (typeId == VariantUtil.PRIMITIVE_TYPE_INT16) {
                return field.getShort();
            }
            return field.getInt();
        }

        @Override
        public long getLong(int i) {
            Variant field = fieldAt(i);
            if (field == null) {
                return 0L;
            }
            int typeId = VariantUtil.primitiveTypeId(field.value(), 0);
            if (typeId == VariantUtil.PRIMITIVE_TYPE_INT8) {
                return field.getByte();
            } else if (typeId == VariantUtil.PRIMITIVE_TYPE_INT16) {
                return field.getShort();
            } else if (typeId == VariantUtil.PRIMITIVE_TYPE_INT32) {
                return field.getInt();
            }
            return field.getLong();
        }

        @Override
        public float getFloat(int i) {
            Variant field = fieldAt(i);
            return field != null ? field.getFloat() : 0f;
        }

        @Override
        public double getDouble(int i) {
            Variant field = fieldAt(i);
            return field != null ? field.getDouble() : 0d;
        }

        @Override
        public Bytes getBytes(int i) {
            Variant field = fieldAt(i);
            if (field == null) {
                return new Bytes(new byte[0], 0, 0);
            }
            byte[] bytes;
            DataTypeRoot root = castType.getTypeRoot();
            if (root == DataTypeRoot.BINARY || root == DataTypeRoot.BYTES) {
                bytes = field.getBinary();
            } else {
                bytes = field.getString().getBytes(StandardCharsets.UTF_8);
            }
            return new Bytes(bytes, 0, bytes.length);
        }

        @Override
        public TimestampNtz getTimestampNtz(int i, int precision) {
            Variant field = fieldAt(i);
            long micros = field != null ? field.getTimestampNtz() : 0L;
            long millis = Math.floorDiv(micros, 1000L);
            int nanosOfMillis = (int) Math.floorMod(micros, 1000L) * 1000;
            return TimestampNtz.fromMillis(millis, nanosOfMillis);
        }

        @Override
        public TimestampLtz getTimestampLtz(int i, int precision) {
            Variant field = fieldAt(i);
            long micros = field != null ? field.getTimestamp() : 0L;
            long millis = Math.floorDiv(micros, 1000L);
            int nanosOfMillis = (int) Math.floorMod(micros, 1000L) * 1000;
            return TimestampLtz.fromEpochMillis(millis, nanosOfMillis);
        }

        @Nullable
        private Variant fieldAt(int rowIndex) {
            if (parent.isNullAt(rowIndex)) {
                return null;
            }
            Variant variant = parent.getVariant(rowIndex);
            if (variant == null || variant.isNull() || !variant.isObject()) {
                return null;
            }
            Variant field = variant.getFieldByName(fieldName);
            if (field == null || field.isNull()) {
                return null;
            }
            return matchesCast(field, castType) ? field : null;
        }
    }

    private static boolean matchesCast(Variant field, DataType castType) {
        int basicType = field.basicType();
        DataTypeRoot root = castType.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return basicType == VariantUtil.BASIC_TYPE_PRIMITIVE
                        && isPrimitiveType(
                                field,
                                VariantUtil.PRIMITIVE_TYPE_TRUE,
                                VariantUtil.PRIMITIVE_TYPE_FALSE);
            case TINYINT:
                return isPrimitiveType(field, VariantUtil.PRIMITIVE_TYPE_INT8);
            case SMALLINT:
                return isPrimitiveType(
                        field, VariantUtil.PRIMITIVE_TYPE_INT8, VariantUtil.PRIMITIVE_TYPE_INT16);
            case INTEGER:
                return isPrimitiveType(
                        field,
                        VariantUtil.PRIMITIVE_TYPE_INT8,
                        VariantUtil.PRIMITIVE_TYPE_INT16,
                        VariantUtil.PRIMITIVE_TYPE_INT32);
            case BIGINT:
                return isPrimitiveType(
                        field,
                        VariantUtil.PRIMITIVE_TYPE_INT8,
                        VariantUtil.PRIMITIVE_TYPE_INT16,
                        VariantUtil.PRIMITIVE_TYPE_INT32,
                        VariantUtil.PRIMITIVE_TYPE_INT64);
            case FLOAT:
                return isPrimitiveType(field, VariantUtil.PRIMITIVE_TYPE_FLOAT);
            case DOUBLE:
                return isPrimitiveType(field, VariantUtil.PRIMITIVE_TYPE_DOUBLE);
            case CHAR:
            case STRING:
                return basicType == VariantUtil.BASIC_TYPE_SHORT_STRING
                        || isPrimitiveType(field, VariantUtil.PRIMITIVE_TYPE_STRING);
            case BINARY:
            case BYTES:
                return isPrimitiveType(field, VariantUtil.PRIMITIVE_TYPE_BINARY);
            case DATE:
                return isPrimitiveType(field, VariantUtil.PRIMITIVE_TYPE_DATE);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return isPrimitiveType(field, VariantUtil.PRIMITIVE_TYPE_TIMESTAMP_NTZ);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return isPrimitiveType(field, VariantUtil.PRIMITIVE_TYPE_TIMESTAMP);
            default:
                return false;
        }
    }

    private static boolean isPrimitiveType(Variant field, int... expectedTypeIds) {
        if (field.basicType() != VariantUtil.BASIC_TYPE_PRIMITIVE) {
            return false;
        }
        int typeId = VariantUtil.primitiveTypeId(field.value(), 0);
        for (int expected : expectedTypeIds) {
            if (typeId == expected) {
                return true;
            }
        }
        return false;
    }

    /**
     * Vector that always reports null. Used when the projected sub-field is missing on the server
     * side or when the cast type does not match the shredded type.
     */
    private static final class AlwaysNullVector
            implements ColumnVector,
                    BooleanColumnVector,
                    ByteColumnVector,
                    ShortColumnVector,
                    IntColumnVector,
                    LongColumnVector,
                    FloatColumnVector,
                    DoubleColumnVector,
                    BytesColumnVector,
                    TimestampNtzColumnVector,
                    TimestampLtzColumnVector {

        private final DataType castType;

        AlwaysNullVector(DataType castType) {
            this.castType = castType;
        }

        @Override
        public boolean isNullAt(int i) {
            return true;
        }

        @Override
        public boolean getBoolean(int i) {
            return false;
        }

        @Override
        public byte getByte(int i) {
            return 0;
        }

        @Override
        public short getShort(int i) {
            return 0;
        }

        @Override
        public int getInt(int i) {
            return 0;
        }

        @Override
        public long getLong(int i) {
            return 0L;
        }

        @Override
        public float getFloat(int i) {
            return 0f;
        }

        @Override
        public double getDouble(int i) {
            return 0d;
        }

        @Override
        public Bytes getBytes(int i) {
            return new Bytes(new byte[0], 0, 0);
        }

        @Override
        public TimestampNtz getTimestampNtz(int i, int precision) {
            return TimestampNtz.fromMillis(0L);
        }

        @Override
        public TimestampLtz getTimestampLtz(int i, int precision) {
            return TimestampLtz.fromEpochMillis(0L);
        }

        @Override
        public String toString() {
            return "AlwaysNullVector(" + castType.asSummaryString() + ")";
        }
    }
}
