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

package org.apache.fluss.lake.lance.writers.array;

import org.apache.fluss.lake.lance.writers.ArrowFieldWriter;
import org.apache.fluss.row.InternalArray;

import org.apache.arrow.vector.Float4Vector;

/** {@link ArrowFieldWriter} for Float elements in arrays. */
public class ArrowArrayFloatWriter extends ArrowFieldWriter<InternalArray> {

    public static ArrowArrayFloatWriter forField(Float4Vector floatVector) {
        return new ArrowArrayFloatWriter(floatVector);
    }

    private ArrowArrayFloatWriter(Float4Vector floatVector) {
        super(floatVector);
    }

    @Override
    public void doWrite(InternalArray array, int ordinal, boolean handleSafe) {
        Float4Vector vector = (Float4Vector) getValueVector();
        if (isNullAt(array, ordinal)) {
            vector.setNull(getCount());
        } else if (handleSafe) {
            vector.setSafe(getCount(), readFloat(array, ordinal));
        } else {
            vector.set(getCount(), readFloat(array, ordinal));
        }
    }

    private boolean isNullAt(InternalArray array, int ordinal) {
        return array.isNullAt(ordinal);
    }

    private float readFloat(InternalArray array, int ordinal) {
        return array.getFloat(ordinal);
    }
}
