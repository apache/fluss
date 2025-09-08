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
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.DataGetters;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarCharVector;

import java.nio.ByteBuffer;

/** {@link ArrowFieldWriter} for VarChar. */
@Internal
public class ArrowVarCharWriter extends ArrowFieldWriter<DataGetters> {

    public static ArrowVarCharWriter forField(VarCharVector varCharVector) {
        return new ArrowVarCharWriter(varCharVector);
    }

    private ArrowVarCharWriter(VarCharVector varCharVector) {
        super(varCharVector);
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        VarCharVector vector = (VarCharVector) getValueVector();
        if (isNullAt(row, ordinal)) {
            vector.setNull(getCount());
        } else {
            ByteBuffer buffer = readString(row, ordinal).wrapByteBuffer();
            vector.setSafe(getCount(), buffer, buffer.position(), buffer.remaining());
        }
    }

    private boolean isNullAt(DataGetters row, int ordinal) {
        return row.isNullAt(ordinal);
    }

    private BinaryString readString(DataGetters row, int ordinal) {
        return row.getString(ordinal);
    }
}
