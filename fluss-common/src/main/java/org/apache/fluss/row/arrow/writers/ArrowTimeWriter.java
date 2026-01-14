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
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeMicroVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeMilliVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeNanoVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeSecVector;

import static org.apache.fluss.utils.Preconditions.checkState;

/** {@link ArrowFieldWriter} for Time. */
@Internal
public class ArrowTimeWriter extends ArrowFieldWriter {

    public ArrowTimeWriter(FieldVector fieldVector) {
        super(fieldVector);
        checkState(
                fieldVector instanceof TimeSecVector
                        || fieldVector instanceof TimeMilliVector
                        || fieldVector instanceof TimeMicroVector
                        || fieldVector instanceof TimeNanoVector);
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        if (fieldVector instanceof TimeSecVector) {
            int sec = readTime(row, ordinal) / 1000;
            if (handleSafe) {
                ((TimeSecVector) fieldVector).setSafe(rowIndex, sec);
            } else {
                ((TimeSecVector) fieldVector).set(rowIndex, sec);
            }
        } else if (fieldVector instanceof TimeMilliVector) {
            int ms = readTime(row, ordinal);
            if (handleSafe) {
                ((TimeMilliVector) fieldVector).setSafe(rowIndex, ms);
            } else {
                ((TimeMilliVector) fieldVector).set(rowIndex, ms);
            }
        } else if (fieldVector instanceof TimeMicroVector) {
            long microSec = readTime(row, ordinal) * 1000L;
            if (handleSafe) {
                ((TimeMicroVector) fieldVector).setSafe(rowIndex, microSec);
            } else {
                ((TimeMicroVector) fieldVector).set(rowIndex, microSec);
            }
        } else {
            long nanoSec = readTime(row, ordinal) * 1000000L;
            if (handleSafe) {
                ((TimeNanoVector) fieldVector).setSafe(rowIndex, nanoSec);
            } else {
                ((TimeNanoVector) fieldVector).set(rowIndex, nanoSec);
            }
        }
    }

    private int readTime(DataGetters row, int ordinal) {
        return row.getInt(ordinal);
    }
}
