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

package org.apache.fluss.row.arrow.writers;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampSecVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType;

import static org.apache.fluss.utils.Preconditions.checkState;

/** {@link ArrowFieldWriter} for TimestampLtz. */
@Internal
public class ArrowTimestampLtzWriter extends ArrowFieldWriter {

    private final int precision;

    public ArrowTimestampLtzWriter(FieldVector fieldVector, int precision) {
        super(fieldVector);
        checkState(
                fieldVector instanceof TimeStampVector
                        && ((ArrowType.Timestamp) fieldVector.getField().getType()).getTimezone()
                                == null);
        this.precision = precision;
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        TimeStampVector vector = (TimeStampVector) fieldVector;
        TimestampLtz timestamp = row.getTimestampLtz(ordinal, precision);
        if (vector instanceof TimeStampSecVector) {
            long sec = timestamp.getEpochMillisecond() / 1000;
            if (handleSafe) {
                vector.setSafe(rowIndex, sec);
            } else {
                vector.set(rowIndex, sec);
            }
        } else if (vector instanceof TimeStampMilliVector) {
            long ms = timestamp.getEpochMillisecond();
            if (handleSafe) {
                vector.setSafe(rowIndex, ms);
            } else {
                vector.set(rowIndex, ms);
            }
        } else if (vector instanceof TimeStampMicroVector) {
            long microSec =
                    timestamp.getEpochMillisecond() * 1000
                            + timestamp.getNanoOfMillisecond() / 1000;
            if (handleSafe) {
                vector.setSafe(rowIndex, microSec);
            } else {
                vector.set(rowIndex, microSec);
            }
        } else {
            long nanoSec =
                    timestamp.getEpochMillisecond() * 1_000_000 + timestamp.getNanoOfMillisecond();
            if (handleSafe) {
                vector.setSafe(rowIndex, nanoSec);
            } else {
                vector.set(rowIndex, nanoSec);
            }
        }
    }
}
