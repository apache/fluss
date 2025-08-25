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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.Float4Vector;

/** {@link ArrowFieldWriter} for Float. */
@Internal
public class ArrowFloatWriter extends ArrowFieldWriter<InternalRow> {
    public static ArrowFloatWriter forField(Float4Vector float4Vector) {
        return new ArrowFloatWriter(float4Vector);
    }

    private ArrowFloatWriter(Float4Vector float4Vector) {
        super(float4Vector);
    }

    @Override
    public void doWrite(InternalRow row, int ordinal, boolean handleSafe) {
        Float4Vector vector = (Float4Vector) getValueVector();
        if (isNullAt(row, ordinal)) {
            vector.setNull(getCount());
        } else if (handleSafe) {
            vector.setSafe(getCount(), readFloat(row, ordinal));
        } else {
            vector.set(getCount(), readFloat(row, ordinal));
        }
    }

    private boolean isNullAt(InternalRow row, int ordinal) {
        return row.isNullAt(ordinal);
    }

    private float readFloat(InternalRow row, int ordinal) {
        return row.getFloat(ordinal);
    }
}
