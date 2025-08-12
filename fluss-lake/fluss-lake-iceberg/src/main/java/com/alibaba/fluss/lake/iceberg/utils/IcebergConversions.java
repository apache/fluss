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

package com.alibaba.fluss.lake.iceberg.utils;

import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Utility class for static conversions between Fluss and Iceberg types. */
public class IcebergConversions {

    /** Convert Fluss TablePath to Iceberg TableIdentifier. */
    public static TableIdentifier toIceberg(TablePath tablePath) {
        return TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    /** Convert Iceberg Type to Flink LogicalType for runtime processing. */
    @SuppressWarnings("checkstyle:SimplifyBooleanExpression")
    public static LogicalType toFlinkLogicalType(Type icebergType) {
        switch (icebergType.typeId()) {
            case BOOLEAN:
                return new BooleanType();
            case INTEGER:
                return new IntType();
            case LONG:
                return new BigIntType();
            case FLOAT:
                return new FloatType();
            case DOUBLE:
                return new DoubleType();
            case STRING:
                return new VarCharType(VarCharType.MAX_LENGTH);
            case BINARY:
                return new BinaryType(BinaryType.MAX_LENGTH);
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) icebergType;
                return new DecimalType(decimal.precision(), decimal.scale());
            case TIMESTAMP:
                Types.TimestampType timestamp = (Types.TimestampType) icebergType;
                return new TimestampType(
                        timestamp.shouldAdjustToUTC(), TimestampType.DEFAULT_PRECISION);
            default:
                throw new UnsupportedOperationException("Unsupported Iceberg type: " + icebergType);
        }
    }
}
