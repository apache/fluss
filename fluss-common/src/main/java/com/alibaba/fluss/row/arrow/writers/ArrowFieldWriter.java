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

package com.alibaba.fluss.row.arrow.writers;

import com.alibaba.fluss.annotation.Internal;

import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ValueVector;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Base class for arrow field writer which is used to convert a field to an Arrow format.
 *
 * @param <IN> Type of the input to write. Currently, it's always InternalRow, may support
 *     InternalArray in the future.
 */
@Internal
public abstract class ArrowFieldWriter<IN> {

    /** Container which is used to store the written sequence of values of a column. */
    private final ValueVector valueVector;

    private int count;

    public ArrowFieldWriter(ValueVector valueVector) {
        this.valueVector = checkNotNull(valueVector);
    }

    /** Returns the underlying container which stores the sequence of values of a column. */
    public ValueVector getValueVector() {
        return valueVector;
    }

    /** Returns the current count of elements written. */
    public int getCount() {
        return count;
    }

    /** Sets the field value as the field at the specified ordinal of the specified row. */
    public abstract void doWrite(IN row, int ordinal, boolean handleSafe);

    /** Writes the specified ordinal of the specified row. */
    public void write(IN row, int ordinal, boolean handleSafe) {
        doWrite(row, ordinal, handleSafe);
        count++;
    }
}
