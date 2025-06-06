/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.TimestampLtz;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toRowKind;

/** To wrap Fluss {@link LogRecord} as paimon {@link InternalRow}. */
public class FlussRecordAsPaimonRow implements InternalRow {

    private final int bucket;
    private final List<String> partitionKeys;
    private final int partitionFieldCount;

    private LogRecord logRecord;
    private int originRowFieldCount;
    private com.alibaba.fluss.row.InternalRow internalRow;
    private Map<String, String> partitionValues;

    public FlussRecordAsPaimonRow(int bucket, List<String> partitionKeys) {
        this.bucket = bucket;
        this.partitionKeys = partitionKeys;
        this.partitionFieldCount = partitionKeys.size();
    }

    public void setFlussRecord(LogRecord logRecord, @Nullable String partitionString) {
        this.logRecord = logRecord;
        this.internalRow = logRecord.getRow();
        this.originRowFieldCount = internalRow.getFieldCount();
        this.partitionValues = parsePartitionString(partitionString);
    }

    @Override
    public int getFieldCount() {
        return originRowFieldCount + partitionFieldCount + 3; // business + partition + system
    }

    @Override
    public RowKind getRowKind() {
        return toRowKind(logRecord.getChangeType());
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        // do nothing
    }

    @Override
    public boolean isNullAt(int pos) {
        if (pos < originRowFieldCount) {
            return internalRow.isNullAt(pos); // Business fields
        } else if (pos < originRowFieldCount + partitionFieldCount) {
            return getPartitionValue(pos) == null; // Partition fields
        } else {
            return false; // System fields (never null)
        }
    }

    @Override
    public boolean getBoolean(int pos) {
        if (pos < originRowFieldCount) {
            return internalRow.getBoolean(pos);
        } else {
            throw new UnsupportedOperationException("Partition and system fields are not boolean");
        }
    }

    @Override
    public byte getByte(int pos) {
        if (pos < originRowFieldCount) {
            return internalRow.getByte(pos);
        } else {
            throw new UnsupportedOperationException("Partition and system fields are not byte");
        }
    }

    @Override
    public short getShort(int pos) {
        if (pos < originRowFieldCount) {
            return internalRow.getShort(pos);
        } else {
            throw new UnsupportedOperationException("Partition and system fields are not short");
        }
    }

    @Override
    public int getInt(int pos) {
        if (pos < originRowFieldCount) {
            return internalRow.getInt(pos); // Business fields
        } else if (pos < originRowFieldCount + partitionFieldCount) {
            throw new UnsupportedOperationException("Partition fields are strings, not ints");
        } else if (pos == originRowFieldCount + partitionFieldCount) {
            return bucket; // System field: bucket
        } else {
            throw new UnsupportedOperationException("Invalid field position for int: " + pos);
        }
    }

    @Override
    public long getLong(int pos) {
        if (pos < originRowFieldCount) {
            return internalRow.getLong(pos); // Business fields
        } else if (pos < originRowFieldCount + partitionFieldCount) {
            throw new UnsupportedOperationException("Partition fields are strings, not longs");
        } else if (pos == originRowFieldCount + partitionFieldCount + 1) {
            return logRecord.logOffset(); // System field: offset
        } else if (pos == originRowFieldCount + partitionFieldCount + 2) {
            return logRecord.timestamp(); // System field: timestamp
        } else {
            throw new UnsupportedOperationException("Invalid field position for long: " + pos);
        }
    }

    @Override
    public float getFloat(int pos) {
        if (pos < originRowFieldCount) {
            return internalRow.getFloat(pos);
        } else {
            throw new UnsupportedOperationException("Partition and system fields are not float");
        }
    }

    @Override
    public double getDouble(int pos) {
        if (pos < originRowFieldCount) {
            return internalRow.getDouble(pos);
        } else {
            throw new UnsupportedOperationException("Partition and system fields are not double");
        }
    }

    @Override
    public BinaryString getString(int pos) {
        if (pos < originRowFieldCount) {
            return BinaryString.fromBytes(internalRow.getString(pos).toBytes()); // Business fields
        } else if (pos < originRowFieldCount + partitionFieldCount) {
            String partitionValue = getPartitionValue(pos); // Partition fields
            return partitionValue != null ? BinaryString.fromString(partitionValue) : null;
        } else {
            throw new UnsupportedOperationException("System fields are not strings");
        }
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        if (pos < originRowFieldCount) {
            com.alibaba.fluss.row.Decimal flussDecimal =
                    internalRow.getDecimal(pos, precision, scale);
            if (flussDecimal.isCompact()) {
                return Decimal.fromUnscaledLong(flussDecimal.toUnscaledLong(), precision, scale);
            } else {
                return Decimal.fromBigDecimal(flussDecimal.toBigDecimal(), precision, scale);
            }
        } else {
            throw new UnsupportedOperationException("Partition and system fields are not decimal");
        }
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        if (pos < originRowFieldCount) {
            if (TimestampLtz.isCompact(precision)) {
                return Timestamp.fromEpochMillis(
                        internalRow.getTimestampLtz(pos, precision).getEpochMillisecond());
            } else {
                TimestampLtz timestampLtz = internalRow.getTimestampLtz(pos, precision);
                return Timestamp.fromEpochMillis(
                        timestampLtz.getEpochMillisecond(), timestampLtz.getNanoOfMillisecond());
            }
        } else if (pos < originRowFieldCount + partitionFieldCount) {
            throw new UnsupportedOperationException("Partition fields are not timestamp");
        } else if (pos == originRowFieldCount + partitionFieldCount + 2) {
            return Timestamp.fromEpochMillis(logRecord.timestamp()); // System field: timestamp
        } else {
            throw new UnsupportedOperationException("Invalid field position for timestamp: " + pos);
        }
    }

    @Override
    public byte[] getBinary(int pos) {
        if (pos < originRowFieldCount) {
            return internalRow.getBytes(pos);
        } else {
            throw new UnsupportedOperationException("Partition and system fields are not binary");
        }
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException(
                "getArray is not support for Fluss record currently.");
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException(
                "getMap is not support for Fluss record currently.");
    }

    @Override
    public InternalRow getRow(int pos, int pos1) {
        throw new UnsupportedOperationException(
                "getRow is not support for Fluss record currently.");
    }

    private String getPartitionValue(int pos) {
        int partitionIndex = pos - originRowFieldCount;
        if (partitionIndex >= 0 && partitionIndex < partitionKeys.size()) {
            String partitionKey = partitionKeys.get(partitionIndex);
            return partitionValues.get(partitionKey);
        }
        return null;
    }

    private Map<String, String> parsePartitionString(@Nullable String partitionString) {
        Map<String, String> values = new LinkedHashMap<>();

        if (partitionString == null || partitionKeys.isEmpty()) {
            return values;
        }

        if (partitionKeys.size() == 1) {
            // Single partition: entire string is the value
            values.put(partitionKeys.get(0), partitionString);
        } else {
            // Multi-partition: parse "key1=value1/key2=value2"
            String[] parts = partitionString.split("/");
            for (String part : parts) {
                String[] keyValue = part.split("=", 2);
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();
                    values.put(key, value);
                }
            }
        }

        return values;
    }
}
