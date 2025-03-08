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
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.Float8Vector;

/** {@link ArrowFieldWriter} for Double. */
@Internal
public class ArrowDoubleWriter extends ArrowFieldWriter<InternalRow> {

    public static ArrowDoubleWriter forField(Float8Vector doubleVector) {
        return new ArrowDoubleWriter(doubleVector);
    }

    private ArrowDoubleWriter(Float8Vector doubleVector) {
        super(doubleVector);
    }

    @Override
    public void doWrite(InternalRow row, int ordinal, boolean handleSafe) {
        Float8Vector vector = (Float8Vector) getValueVector();
        if (isNullAt(row, ordinal)) {
            vector.setNull(getCount());
        } else if (handleSafe) {
            vector.setSafe(getCount(), readDouble(row, ordinal));
        } else {
            vector.set(getCount(), readDouble(row, ordinal));
        }
    }

    private boolean isNullAt(InternalRow row, int ordinal) {
        return row.isNullAt(ordinal);
    }

    private double readDouble(InternalRow row, int ordinal) {
        return row.getDouble(ordinal);
    }
}
