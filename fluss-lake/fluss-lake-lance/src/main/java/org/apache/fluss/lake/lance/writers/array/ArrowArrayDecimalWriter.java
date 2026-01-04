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

import org.apache.arrow.vector.DecimalVector;

import java.math.BigDecimal;

/** {@link ArrowFieldWriter} for Decimal elements in arrays. */
public class ArrowArrayDecimalWriter extends ArrowFieldWriter<InternalArray> {

    private final int precision;
    private final int scale;

    public static ArrowArrayDecimalWriter forField(
            DecimalVector decimalVector, int precision, int scale) {
        return new ArrowArrayDecimalWriter(decimalVector, precision, scale);
    }

    private ArrowArrayDecimalWriter(DecimalVector decimalVector, int precision, int scale) {
        super(decimalVector);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public void doWrite(InternalArray array, int ordinal, boolean handleSafe) {
        DecimalVector vector = (DecimalVector) getValueVector();
        if (isNullAt(array, ordinal)) {
            vector.setNull(getCount());
        } else {
            BigDecimal decimal = readDecimal(array, ordinal, precision, scale).toBigDecimal();
            if (handleSafe) {
                vector.setSafe(getCount(), decimal);
            } else {
                vector.set(getCount(), decimal);
            }
        }
    }

    private boolean isNullAt(InternalArray array, int ordinal) {
        return array.isNullAt(ordinal);
    }

    private org.apache.fluss.row.Decimal readDecimal(
            InternalArray array, int ordinal, int precision, int scale) {
        return array.getDecimal(ordinal, precision, scale);
    }
}
