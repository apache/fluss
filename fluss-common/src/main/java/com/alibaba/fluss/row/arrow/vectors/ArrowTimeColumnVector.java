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
import com.alibaba.fluss.row.columnar.IntColumnVector;

import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeMicroVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeMilliVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeNanoVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeSecVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ValueVector;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/** Arrow column vector for Time. */
@Internal
public class ArrowTimeColumnVector implements IntColumnVector {

    /** Container which is used to store the sequence of time values of a column to read. */
    private final ValueVector valueVector;

    public ArrowTimeColumnVector(ValueVector valueVector) {
        this.valueVector = checkNotNull(valueVector);
        checkState(
                valueVector instanceof TimeSecVector
                        || valueVector instanceof TimeMilliVector
                        || valueVector instanceof TimeMicroVector
                        || valueVector instanceof TimeNanoVector);
    }

    @Override
    public int getInt(int i) {
        if (valueVector instanceof TimeSecVector) {
            return ((TimeSecVector) valueVector).get(i) * 1000;
        } else if (valueVector instanceof TimeMilliVector) {
            return ((TimeMilliVector) valueVector).get(i);
        } else if (valueVector instanceof TimeMicroVector) {
            return (int) (((TimeMicroVector) valueVector).get(i) / 1000);
        } else {
            return (int) (((TimeNanoVector) valueVector).get(i) / 1000000);
        }
    }

    @Override
    public boolean isNullAt(int i) {
        return valueVector.isNull(i);
    }
}
