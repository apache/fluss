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

package com.alibaba.fluss.testutils;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;

import java.io.IOException;
import java.math.BigDecimal;

import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;

/** Util class to create {@link IndexedRow} with different data types. */
public class RowUtils {

    // ---- Helper methods to create IndexedRow instances ----
    public static IndexedRow createRowWithInt(int value) throws IOException {
        DataType[] dataTypes = {DataTypes.INT()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeInt(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithLong(long value) throws IOException {
        DataType[] dataTypes = {DataTypes.BIGINT()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeLong(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithString(String value) throws IOException {
        DataType[] dataTypes = {DataTypes.STRING()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeString(BinaryString.fromString(value));
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithBoolean(boolean value) throws IOException {
        DataType[] dataTypes = {DataTypes.BOOLEAN()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeBoolean(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithFloat(float value) throws IOException {
        DataType[] dataTypes = {DataTypes.FLOAT()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeFloat(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithDouble(double value) throws IOException {
        DataType[] dataTypes = {DataTypes.DOUBLE()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeDouble(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithDecimal(BigDecimal value, int precision, int scale)
            throws IOException {
        DataType[] dataTypes = {DataTypes.DECIMAL(precision, scale)};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeDecimal(Decimal.fromBigDecimal(value, precision, scale), precision);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithTimestampNtz(long millis, int nanos) throws IOException {
        DataType[] dataTypes = {DataTypes.TIMESTAMP(6)};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeTimestampNtz(TimestampNtz.fromMillis(millis, nanos), 6);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithTimestampLtz(long millis, int nanos) throws IOException {
        DataType[] dataTypes = {DataTypes.TIMESTAMP_LTZ(6)};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeTimestampLtz(TimestampLtz.fromEpochMillis(millis, nanos), 6);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithDate(int days) throws IOException {
        DataType[] dataTypes = {DataTypes.DATE()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeInt(days);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithTime(int millis) throws IOException {
        DataType[] dataTypes = {DataTypes.TIME()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeInt(millis); // Fluss stores TIME as int (milliseconds)
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            writer.close();
            return row;
        }
    }

    public static IndexedRow createRowWithBytes(byte[] value) throws IOException {
        DataType[] dataTypes = {DataTypes.BYTES()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeBytes(value);
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }

    public static IndexedRow createRowWithTimestampNtz(long millis, int nanos, DataType type)
            throws IOException {
        DataType[] dataTypes = {DataTypes.BYTES()};
        try (IndexedRowWriter writer = new IndexedRowWriter(dataTypes)) {
            writer.writeTimestampNtz(TimestampNtz.fromMillis(millis, nanos), getPrecision(type));
            IndexedRow row = new IndexedRow(new DataType[] {type});
            row.pointTo(writer.segment(), 0, writer.position());
            return row;
        }
    }
}
