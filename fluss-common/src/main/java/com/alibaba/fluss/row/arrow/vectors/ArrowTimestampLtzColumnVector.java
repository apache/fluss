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

package com.alibaba.fluss.row.arrow.vectors;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.columnar.TimestampLtzColumnVector;

import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampSecVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ValueVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/** Arrow column vector for TimestampLtz. */
@Internal
public class ArrowTimestampLtzColumnVector implements TimestampLtzColumnVector {

    /**
     * Container which is used to store the sequence of {@link TimestampLtz} values of a column to
     * read.
     */
    private final ValueVector valueVector;

    public ArrowTimestampLtzColumnVector(ValueVector valueVector) {
        this.valueVector = checkNotNull(valueVector);
        checkState(
                valueVector instanceof TimeStampVector
                        && ((ArrowType.Timestamp) valueVector.getField().getType()).getTimezone()
                                == null);
    }

    @Override
    public TimestampLtz getTimestampLtz(int i, int precision) {
        if (valueVector instanceof TimeStampSecVector) {
            return TimestampLtz.fromEpochMillis(((TimeStampSecVector) valueVector).get(i) * 1000);
        } else if (valueVector instanceof TimeStampMilliVector) {
            return TimestampLtz.fromEpochMillis(((TimeStampMilliVector) valueVector).get(i));
        } else if (valueVector instanceof TimeStampMicroVector) {
            long micros = ((TimeStampMicroVector) valueVector).get(i);
            return TimestampLtz.fromEpochMillis(micros / 1000, (int) (micros % 1000) * 1000);
        } else {
            long nanos = ((TimeStampNanoVector) valueVector).get(i);
            return TimestampLtz.fromEpochMillis(nanos / 1_000_000, (int) (nanos % 1_000_000));
        }
    }

    @Override
    public boolean isNullAt(int i) {
        return valueVector.isNull(i);
    }
}
