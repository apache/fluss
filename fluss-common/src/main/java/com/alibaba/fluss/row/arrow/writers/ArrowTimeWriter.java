/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row.arrow.writers;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.BaseFixedWidthVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TimeMicroVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TimeMilliVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TimeNanoVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TimeSecVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ValueVector;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/** {@link ArrowFieldWriter} for Time. */
@Internal
public class ArrowTimeWriter extends ArrowFieldWriter<InternalRow> {

    public static ArrowTimeWriter forField(ValueVector valueVector) {
        return new ArrowTimeWriter(valueVector);
    }

    private ArrowTimeWriter(ValueVector valueVector) {
        super(valueVector);
        checkState(
                valueVector instanceof TimeSecVector
                        || valueVector instanceof TimeMilliVector
                        || valueVector instanceof TimeMicroVector
                        || valueVector instanceof TimeNanoVector);
    }

    @Override
    public void doWrite(InternalRow row, int ordinal, boolean handleSafe) {
        ValueVector valueVector = getValueVector();
        if (isNullAt(row, ordinal)) {
            ((BaseFixedWidthVector) valueVector).setNull(getCount());
        } else if (valueVector instanceof TimeSecVector) {
            int sec = readTime(row, ordinal) / 1000;
            if (handleSafe) {
                ((TimeSecVector) valueVector).setSafe(getCount(), sec);
            } else {
                ((TimeSecVector) valueVector).set(getCount(), sec);
            }
        } else if (valueVector instanceof TimeMilliVector) {
            int ms = readTime(row, ordinal);
            if (handleSafe) {
                ((TimeMilliVector) valueVector).setSafe(getCount(), ms);
            } else {
                ((TimeMilliVector) valueVector).set(getCount(), ms);
            }
        } else if (valueVector instanceof TimeMicroVector) {
            long microSec = readTime(row, ordinal) * 1000L;
            if (handleSafe) {
                ((TimeMicroVector) valueVector).setSafe(getCount(), microSec);
            } else {
                ((TimeMicroVector) valueVector).set(getCount(), microSec);
            }
        } else {
            long nanoSec = readTime(row, ordinal) * 1000000L;
            if (handleSafe) {
                ((TimeNanoVector) valueVector).setSafe(getCount(), nanoSec);
            } else {
                ((TimeNanoVector) valueVector).set(getCount(), nanoSec);
            }
        }
    }

    private boolean isNullAt(InternalRow row, int ordinal) {
        return row.isNullAt(ordinal);
    }

    private int readTime(InternalRow row, int ordinal) {
        return row.getInt(ordinal);
    }
}
