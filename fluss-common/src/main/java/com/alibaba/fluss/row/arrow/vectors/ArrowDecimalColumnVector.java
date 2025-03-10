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

package com.alibaba.fluss.row.arrow.vectors;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.columnar.DecimalColumnVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.DecimalVector;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Arrow column vector for Decimal. */
@Internal
public class ArrowDecimalColumnVector implements DecimalColumnVector {

    /** Container which is used to store the sequence of DecimalData values of a column to read. */
    private final DecimalVector decimalVector;

    public ArrowDecimalColumnVector(DecimalVector decimalVector) {
        this.decimalVector = checkNotNull(decimalVector);
    }

    @Override
    public Decimal getDecimal(int i, int precision, int scale) {
        return Decimal.fromBigDecimal(decimalVector.getObject(i), precision, scale);
    }

    @Override
    public boolean isNullAt(int i) {
        return decimalVector.isNull(i);
    }
}
