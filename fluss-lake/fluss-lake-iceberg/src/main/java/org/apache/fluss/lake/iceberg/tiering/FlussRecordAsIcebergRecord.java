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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.iceberg.source.FlussRowAsIcebergRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.apache.fluss.lake.iceberg.IcebergLakeCatalog.SYSTEM_COLUMNS;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Wrap Fluss {@link LogRecord} as Iceberg {@link Record}.
 *
 * <p>todo: refactor to implement ParquetWriters, OrcWriters, AvroWriters just like Flink & Spark
 * write to iceberg for higher performance
 */
public class FlussRecordAsIcebergRecord extends FlussRowAsIcebergRecord {

    // Lake table for iceberg will append three system columns: __bucket, __offset,__timestamp
    private static final int LAKE_ICEBERG_SYSTEM_COLUMNS = SYSTEM_COLUMNS.size();

    private LogRecord logRecord;
    private final int bucket;

    // the origin row fields in fluss, excluding the system columns in iceberg
    private int originRowFieldCount;

    public FlussRecordAsIcebergRecord(
            int bucket, Types.StructType structType, RowType flussRowType) {
        super(structType, flussRowType);
        this.bucket = bucket;
    }

    public void setFlussRecord(LogRecord logRecord) {
        this.logRecord = logRecord;
        this.internalRow = logRecord.getRow();
        this.originRowFieldCount = internalRow.getFieldCount();
        checkState(
                originRowFieldCount == structType.fields().size() - LAKE_ICEBERG_SYSTEM_COLUMNS,
                "The Iceberg table fields count must equals to LogRecord's fields count.");
    }

    @Override
    public Object getField(String name) {
        if (SYSTEM_COLUMNS.containsKey(name)) {
            switch (name) {
                case BUCKET_COLUMN_NAME:
                    return bucket;
                case OFFSET_COLUMN_NAME:
                    return logRecord.logOffset();
                case TIMESTAMP_COLUMN_NAME:
                    return toIcebergTimestampLtz(logRecord.timestamp());
                default:
                    throw new IllegalArgumentException("Unknown system column: " + name);
            }
        }
        return super.getField(name);
    }

    @Override
    public Object get(int pos) {
        // firstly, for system columns
        if (pos == originRowFieldCount) {
            // bucket column
            return bucket;
        } else if (pos == originRowFieldCount + 1) {
            // log offset column
            return logRecord.logOffset();
        } else if (pos == originRowFieldCount + 2) {
            // timestamp column
            return toIcebergTimestampLtz(logRecord.timestamp());
        }
        return super.get(pos);
    }

    private OffsetDateTime toIcebergTimestampLtz(long timestamp) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
    }
}
