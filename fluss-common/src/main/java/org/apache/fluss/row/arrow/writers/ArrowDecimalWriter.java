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
import org.apache.fluss.row.Decimal;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.DecimalVector;

import java.math.BigDecimal;

/** {@link ArrowFieldWriter} for Decimal. */
@Internal
public class ArrowDecimalWriter extends ArrowFieldWriter<DataGetters> {

    public static ArrowDecimalWriter forField(
            DecimalVector decimalVector, int precision, int scale) {
        return new ArrowDecimalWriter(decimalVector, precision, scale);
    }

    private final int precision;
    private final int scale;

    private ArrowDecimalWriter(DecimalVector decimalVector, int precision, int scale) {
        super(decimalVector);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        DecimalVector vector = (DecimalVector) getValueVector();
        if (isNullAt(row, ordinal)) {
            vector.setNull(getCount());
        } else {
            BigDecimal bigDecimal = readDecimal(row, ordinal).toBigDecimal();
            if (bigDecimal == null) {
                vector.setNull(getCount());
            } else {
                if (handleSafe) {
                    vector.setSafe(getCount(), bigDecimal);
                } else {
                    vector.set(getCount(), bigDecimal);
                }
            }
        }
    }

    private boolean isNullAt(DataGetters row, int ordinal) {
        return row.isNullAt(ordinal);
    }

    private Decimal readDecimal(DataGetters row, int ordinal) {
        return row.getDecimal(ordinal, precision, scale);
    }
}
