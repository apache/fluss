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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import org.apache.paimon.types.DataType;

import java.util.ArrayList;
import java.util.List;

/** Convert from Paimon's data type to Fluss's data type. */
public class PaimonDataTypeToFlussDataType {

    public static final PaimonDataTypeToFlussDataType INSTANCE =
            new PaimonDataTypeToFlussDataType();

    /** Converts a Paimon {@link DataType} to a Fluss {@link org.apache.fluss.types.DataType}. */
    public org.apache.fluss.types.DataType toFlussType(DataType paimonType) {
        boolean nullable = paimonType.isNullable();
        switch (paimonType.getTypeRoot()) {
            case BOOLEAN:
                return new BooleanType(nullable);
            case TINYINT:
                return new TinyIntType(nullable);
            case SMALLINT:
                return new SmallIntType(nullable);
            case INTEGER:
                return new IntType(nullable);
            case BIGINT:
                return new BigIntType(nullable);
            case FLOAT:
                return new FloatType(nullable);
            case DOUBLE:
                return new DoubleType(nullable);
            case CHAR:
                return new CharType(
                        nullable, ((org.apache.paimon.types.CharType) paimonType).getLength());
            case VARCHAR:
                return new StringType(nullable);
            case BINARY:
                return new BinaryType(
                        nullable, ((org.apache.paimon.types.BinaryType) paimonType).getLength());
            case VARBINARY:
                return new BytesType(nullable);
            case DECIMAL:
                org.apache.paimon.types.DecimalType pdt =
                        (org.apache.paimon.types.DecimalType) paimonType;
                return new DecimalType(nullable, pdt.getPrecision(), pdt.getScale());
            case DATE:
                return new DateType(nullable);
            case TIME_WITHOUT_TIME_ZONE:
                org.apache.paimon.types.TimeType pTimeType =
                        (org.apache.paimon.types.TimeType) paimonType;
                return new TimeType(nullable, pTimeType.getPrecision());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                org.apache.paimon.types.TimestampType ptt =
                        (org.apache.paimon.types.TimestampType) paimonType;
                return new TimestampType(nullable, ptt.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                org.apache.paimon.types.LocalZonedTimestampType plzt =
                        (org.apache.paimon.types.LocalZonedTimestampType) paimonType;
                return new LocalZonedTimestampType(nullable, plzt.getPrecision());
            case ARRAY:
                org.apache.paimon.types.ArrayType pat =
                        (org.apache.paimon.types.ArrayType) paimonType;
                return new ArrayType(nullable, toFlussType(pat.getElementType()));
            case MAP:
                org.apache.paimon.types.MapType pmt = (org.apache.paimon.types.MapType) paimonType;
                return new MapType(
                        nullable, toFlussType(pmt.getKeyType()), toFlussType(pmt.getValueType()));
            case ROW:
                org.apache.paimon.types.RowType prt = (org.apache.paimon.types.RowType) paimonType;
                List<org.apache.paimon.types.DataField> paimonFields = prt.getFields();
                List<DataField> flussFields = new ArrayList<>(paimonFields.size());
                for (org.apache.paimon.types.DataField pf : paimonFields) {
                    flussFields.add(
                            new DataField(pf.name(), toFlussType(pf.type()), pf.description()));
                }
                return new RowType(nullable, flussFields);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Paimon data type: " + paimonType);
        }
    }
}
