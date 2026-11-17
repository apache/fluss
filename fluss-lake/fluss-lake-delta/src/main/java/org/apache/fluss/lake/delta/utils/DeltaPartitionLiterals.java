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

package org.apache.fluss.lake.delta.utils;

import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.PartitionUtils;

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Converts between the partition values of a Fluss partition name and the Delta Kernel literals of
 * the partition columns. Kernel checks that each literal has the type of its partition column, and
 * serializes it into the data file path and the add action.
 */
public class DeltaPartitionLiterals {

    /**
     * The Fluss partition key types that can be Delta partition columns. Delta has no TIME type,
     * and it encodes binary partition values as text, which can't hold arbitrary bytes.
     */
    private static final Set<DataTypeRoot> SUPPORTED_TYPES =
            EnumSet.of(
                    DataTypeRoot.CHAR,
                    DataTypeRoot.STRING,
                    DataTypeRoot.BOOLEAN,
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT,
                    DataTypeRoot.FLOAT,
                    DataTypeRoot.DOUBLE,
                    DataTypeRoot.DATE,
                    DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

    private DeltaPartitionLiterals() {}

    /** Returns whether a Fluss partition key of the given type can be a Delta partition column. */
    public static boolean isSupported(DataTypeRoot type) {
        return SUPPORTED_TYPES.contains(type);
    }

    /**
     * Converts partition values keyed by partition column, in the format of Fluss partition names,
     * to literals of the column types in the given Fluss row type.
     */
    public static Map<String, Literal> toLiterals(
            Map<String, String> partitionValues, RowType rowType) {
        Map<String, Literal> literals = new HashMap<>();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            DataTypeRoot type =
                    rowType.getTypeAt(rowType.getFieldIndex(entry.getKey())).getTypeRoot();
            literals.put(entry.getKey(), toLiteral(entry.getValue(), type));
        }
        return literals;
    }

    /** Converts one partition value of a Fluss partition name to a literal of the given type. */
    public static Literal toLiteral(String value, DataTypeRoot type) {
        Object parsed = PartitionUtils.parseValueOfType(value, type);
        switch (type) {
            case CHAR:
            case STRING:
                return Literal.ofString(value);
            case BOOLEAN:
                return Literal.ofBoolean((Boolean) parsed);
            case TINYINT:
                return Literal.ofByte((Byte) parsed);
            case SMALLINT:
                return Literal.ofShort((Short) parsed);
            case INTEGER:
                return Literal.ofInt((Integer) parsed);
            case BIGINT:
                return Literal.ofLong((Long) parsed);
            case FLOAT:
                return Literal.ofFloat((Float) parsed);
            case DOUBLE:
                return Literal.ofDouble((Double) parsed);
            case DATE:
                return Literal.ofDate((Integer) parsed);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampNtz ntz = (TimestampNtz) parsed;
                return Literal.ofTimestampNtz(
                        ntz.getMillisecond() * 1000 + ntz.getNanoOfMillisecond() / 1000);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Literal.ofTimestamp(((TimestampLtz) parsed).toEpochMicros());
            default:
                throw new UnsupportedOperationException(
                        "Delta Lake partition columns don't support the type " + type);
        }
    }

    /**
     * Converts a partition literal of a Delta partition column to the value in a Fluss partition
     * name, the inverse of {@link #toLiteral}. A Fluss CHAR partition key is a Delta STRING column
     * and has the same value in its partition name.
     */
    public static String toFlussPartitionValue(Literal literal) {
        Object value = literal.getValue();
        DataType type = literal.getDataType();
        if (value == null) {
            throw new IllegalArgumentException(
                    "A Fluss partition can't have a null value, but the Delta partition value of "
                            + "type "
                            + type
                            + " is null.");
        }
        if (type instanceof StringType) {
            return (String) value;
        } else if (type instanceof BooleanType) {
            return PartitionUtils.convertValueOfType(value, DataTypeRoot.BOOLEAN);
        } else if (type instanceof ByteType) {
            return PartitionUtils.convertValueOfType(value, DataTypeRoot.TINYINT);
        } else if (type instanceof ShortType) {
            return PartitionUtils.convertValueOfType(value, DataTypeRoot.SMALLINT);
        } else if (type instanceof IntegerType) {
            return PartitionUtils.convertValueOfType(value, DataTypeRoot.INTEGER);
        } else if (type instanceof LongType) {
            return PartitionUtils.convertValueOfType(value, DataTypeRoot.BIGINT);
        } else if (type instanceof FloatType) {
            return PartitionUtils.convertValueOfType(value, DataTypeRoot.FLOAT);
        } else if (type instanceof DoubleType) {
            return PartitionUtils.convertValueOfType(value, DataTypeRoot.DOUBLE);
        } else if (type instanceof DateType) {
            return PartitionUtils.convertValueOfType(value, DataTypeRoot.DATE);
        } else if (type instanceof TimestampNTZType) {
            return PartitionUtils.convertValueOfType(
                    TimestampNtz.fromMicros((Long) value),
                    DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        } else if (type instanceof TimestampType) {
            return PartitionUtils.convertValueOfType(
                    TimestampLtz.fromEpochMicros((Long) value),
                    DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        }
        throw new UnsupportedOperationException(
                "Fluss partitions don't support Delta partition columns of type " + type);
    }
}
