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

package org.apache.fluss.types;

import java.util.List;

/**
 * Utilities for checking {@link DataType} and avoiding a lot of type casting and repetitive work.
 */
public final class DataTypeChecks {
    private static final LengthExtractor LENGTH_EXTRACTOR = new LengthExtractor();

    private static final PrecisionExtractor PRECISION_EXTRACTOR = new PrecisionExtractor();

    private static final ScaleExtractor SCALE_EXTRACTOR = new ScaleExtractor();

    private static final FieldCountExtractor FIELD_COUNT_EXTRACTOR = new FieldCountExtractor();

    private static final FieldTypesExtractor FIELD_TYPES_EXTRACTOR = new FieldTypesExtractor();

    public static int getLength(DataType dataType) {
        return dataType.accept(LENGTH_EXTRACTOR);
    }

    /** Returns the precision of all types that define a precision implicitly or explicitly. */
    public static int getPrecision(DataType dataType) {
        return dataType.accept(PRECISION_EXTRACTOR);
    }

    /** Returns the scale of all types that define a scale implicitly or explicitly. */
    public static int getScale(DataType dataType) {
        return dataType.accept(SCALE_EXTRACTOR);
    }

    /** Returns the field count of row and structured types. Other types return 1. */
    public static int getFieldCount(DataType dataType) {
        return dataType.accept(FIELD_COUNT_EXTRACTOR);
    }

    /** Returns the field types of row and structured types. */
    public static List<DataType> getFieldTypes(DataType dataType) {
        return dataType.accept(FIELD_TYPES_EXTRACTOR);
    }

    private DataTypeChecks() {
        // no instantiation
    }

    // --------------------------------------------------------------------------------------------
    /** Extracts an attribute of data types that define that attribute. */
    private static class Extractor<T> extends DataTypeDefaultVisitor<T> {
        @Override
        protected T defaultMethod(DataType dataType) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid use of extractor %s. Called on data type: %s",
                            this.getClass().getSimpleName(), dataType));
        }
    }

    private static class LengthExtractor extends Extractor<Integer> {
        @Override
        public Integer visit(CharType charType) {
            return charType.getLength();
        }

        @Override
        public Integer visit(BinaryType binaryType) {
            return binaryType.getLength();
        }
    }

    private static class PrecisionExtractor extends Extractor<Integer> {
        @Override
        public Integer visit(DecimalType decimalType) {
            return decimalType.getPrecision();
        }

        @Override
        public Integer visit(TimeType timeType) {
            return timeType.getPrecision();
        }

        @Override
        public Integer visit(TimestampType timestampType) {
            return timestampType.getPrecision();
        }

        @Override
        public Integer visit(LocalZonedTimestampType localZonedTimestampType) {
            return localZonedTimestampType.getPrecision();
        }
    }

    private static class ScaleExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(DecimalType decimalType) {
            return decimalType.getScale();
        }

        @Override
        public Integer visit(TinyIntType tinyIntType) {
            return 0;
        }

        @Override
        public Integer visit(SmallIntType smallIntType) {
            return 0;
        }

        @Override
        public Integer visit(IntType intType) {
            return 0;
        }

        @Override
        public Integer visit(BigIntType bigIntType) {
            return 0;
        }
    }

    private static class FieldCountExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(RowType rowType) {
            return rowType.getFieldCount();
        }

        @Override
        protected Integer defaultMethod(DataType dataType) {
            return 1;
        }
    }

    private static class FieldTypesExtractor extends Extractor<List<DataType>> {

        @Override
        public List<DataType> visit(RowType rowType) {
            return rowType.getFieldTypes();
        }
    }
}
