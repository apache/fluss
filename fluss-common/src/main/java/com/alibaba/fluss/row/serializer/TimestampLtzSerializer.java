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

package com.alibaba.fluss.row.serializer;

import com.alibaba.fluss.memory.InputView;
import com.alibaba.fluss.memory.OutputView;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.utils.DateTimeUtils;

import java.io.IOException;

/**
 * Serializer for {@link TimestampLtz}.
 *
 * <p>A {@link TimestampLtz} instance can be compactly serialized as a long value(= millisecond)
 * when the Timestamp type is compact. Otherwise it's serialized as a long value and a int value.
 */
public class TimestampLtzSerializer implements Serializer<TimestampLtz> {

    private static final long serialVersionUID = 1L;

    private final int precision;

    public TimestampLtzSerializer(int precision) {
        this.precision = precision;
    }

    @Override
    public Serializer<TimestampLtz> duplicate() {
        return new TimestampLtzSerializer(precision);
    }

    @Override
    public TimestampLtz copy(TimestampLtz from) {
        return from;
    }

    @Override
    public void serialize(TimestampLtz record, OutputView target) throws IOException {
        if (TimestampLtz.isCompact(precision)) {
            assert record.getNanoOfMillisecond() == 0;
            target.writeLong(record.getEpochMillisecond());
        } else {
            target.writeLong(record.getEpochMillisecond());
            target.writeInt(record.getNanoOfMillisecond());
        }
    }

    @Override
    public TimestampLtz deserialize(InputView source) throws IOException {
        if (TimestampLtz.isCompact(precision)) {
            long val = source.readLong();
            return TimestampLtz.fromEpochMillis(val);
        } else {
            long longVal = source.readLong();
            int intVal = source.readInt();
            return TimestampLtz.fromEpochMillis(longVal, intVal);
        }
    }

    @Override
    public String serializeToString(TimestampLtz record) {
        return DateTimeUtils.formatTimestampLtz(record, precision);
    }

    @Override
    public TimestampLtz deserializeFromString(String s) {
        return DateTimeUtils.parseTimestampLtzData(s, precision);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TimestampLtzSerializer that = (TimestampLtzSerializer) obj;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return precision;
    }
}
