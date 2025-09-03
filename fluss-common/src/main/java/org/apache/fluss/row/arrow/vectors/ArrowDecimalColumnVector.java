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

package org.apache.fluss.row.arrow.vectors;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.columnar.DecimalColumnVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.DecimalVector;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

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
