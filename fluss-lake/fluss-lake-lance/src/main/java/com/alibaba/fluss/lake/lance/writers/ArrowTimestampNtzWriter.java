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

package com.alibaba.fluss.lake.lance.writers;

import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampNtz;

import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/** {@link ArrowFieldWriter} for TimestampNtz. */
public class ArrowTimestampNtzWriter extends ArrowFieldWriter<InternalRow> {
    public static ArrowTimestampNtzWriter forField(ValueVector valueVector, int precision) {
        return new ArrowTimestampNtzWriter(valueVector, precision);
    }

    private final int precision;

    private ArrowTimestampNtzWriter(ValueVector valueVector, int precision) {
        super(valueVector);
        checkState(
                valueVector instanceof TimeStampVector
                        && ((ArrowType.Timestamp) valueVector.getField().getType()).getTimezone()
                                == null);
        this.precision = precision;
    }

    @Override
    public void doWrite(InternalRow row, int ordinal, boolean handleSafe) {
        TimeStampVector vector = (TimeStampVector) getValueVector();
        if (isNullAt(row, ordinal)) {
            vector.setNull(getCount());
        } else {
            TimestampNtz timestamp = readTimestamp(row, ordinal);
            if (vector instanceof TimeStampSecVector) {
                long sec = timestamp.getMillisecond() / 1000;
                if (handleSafe) {
                    vector.setSafe(getCount(), sec);
                } else {
                    vector.set(getCount(), sec);
                }
            } else if (vector instanceof TimeStampMilliVector) {
                long ms = timestamp.getMillisecond();
                if (handleSafe) {
                    vector.setSafe(getCount(), ms);
                } else {
                    vector.set(getCount(), ms);
                }
            } else if (vector instanceof TimeStampMicroVector) {
                long microSec =
                        timestamp.getMillisecond() * 1000 + timestamp.getNanoOfMillisecond() / 1000;
                if (handleSafe) {
                    vector.setSafe(getCount(), microSec);
                } else {
                    vector.set(getCount(), microSec);
                }
            } else {
                long nanoSec =
                        timestamp.getMillisecond() * 1_000_000 + timestamp.getNanoOfMillisecond();
                if (handleSafe) {
                    vector.setSafe(getCount(), nanoSec);
                } else {
                    vector.set(getCount(), nanoSec);
                }
            }
        }
    }

    private boolean isNullAt(InternalRow row, int ordinal) {
        return row.isNullAt(ordinal);
    }

    private TimestampNtz readTimestamp(InternalRow row, int ordinal) {
        return row.getTimestampNtz(ordinal, precision);
    }
}
