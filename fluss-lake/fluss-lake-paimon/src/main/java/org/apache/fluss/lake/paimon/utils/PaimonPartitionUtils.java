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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.utils.PartitionNameConverters;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

/** Extracts partition values from a Paimon partition row as Fluss partition-name strings. */
public class PaimonPartitionUtils {

    private PaimonPartitionUtils() {}

    /**
     * Converts a Paimon partition row into Fluss partition value strings, in partition-key order.
     * The output must match {@code PartitionUtils#convertValueOfType} (the Fluss-side format),
     * otherwise lake-side and Fluss-side partition names won't match during union read split
     * generation.
     */
    public static List<String> partitionValues(BinaryRow partition, RowType partitionType) {
        List<String> values = new ArrayList<>(partition.getFieldCount());
        for (int i = 0; i < partition.getFieldCount(); i++) {
            values.add(toFlussPartitionString(partition, i, partitionType.getTypeAt(i)));
        }
        return values;
    }

    private static String toFlussPartitionString(BinaryRow partition, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return partition.getString(pos).toString();
            case BOOLEAN:
                return Boolean.toString(partition.getBoolean(pos));
            case TINYINT:
                return Byte.toString(partition.getByte(pos));
            case SMALLINT:
                return Short.toString(partition.getShort(pos));
            case INTEGER:
                return Integer.toString(partition.getInt(pos));
            case BIGINT:
                return Long.toString(partition.getLong(pos));
            case FLOAT:
                return PartitionNameConverters.reformatFloat(partition.getFloat(pos));
            case DOUBLE:
                return PartitionNameConverters.reformatDouble(partition.getDouble(pos));
            case DATE:
                return PartitionNameConverters.dayToString(partition.getInt(pos));
            case TIME_WITHOUT_TIME_ZONE:
                return PartitionNameConverters.milliToString(partition.getInt(pos));
            case BINARY:
            case VARBINARY:
                return PartitionNameConverters.hexString(partition.getBinary(pos));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    Timestamp ts = partition.getTimestamp(pos, DataTypeChecks.getPrecision(type));
                    return PartitionNameConverters.timestampToString(
                            TimestampNtz.fromMillis(
                                    ts.getMillisecond(), ts.getNanoOfMillisecond()));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    Timestamp ts = partition.getTimestamp(pos, DataTypeChecks.getPrecision(type));
                    return PartitionNameConverters.timestampToString(
                            TimestampLtz.fromEpochMillis(
                                    ts.getMillisecond(), ts.getNanoOfMillisecond()));
                }
            default:
                throw new IllegalArgumentException(
                        "Unsupported partition column type: " + type.getTypeRoot());
        }
    }
}
