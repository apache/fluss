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

package com.alibaba.fluss.lake.lance.writers;

import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;

import org.apache.arrow.vector.DecimalVector;

import java.math.BigDecimal;

/** {@link ArrowFieldWriter} for Decimal. */
public class ArrowDecimalWriter extends ArrowFieldWriter<InternalRow> {

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
    public void doWrite(InternalRow row, int ordinal, boolean handleSafe) {
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

    private boolean isNullAt(InternalRow row, int ordinal) {
        return row.isNullAt(ordinal);
    }

    private Decimal readDecimal(InternalRow row, int ordinal) {
        return row.getDecimal(ordinal, precision, scale);
    }
}
