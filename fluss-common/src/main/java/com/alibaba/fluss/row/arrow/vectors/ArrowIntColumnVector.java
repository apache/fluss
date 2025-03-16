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
import com.alibaba.fluss.row.columnar.IntColumnVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Arrow column vector for Int. */
@Internal
public class ArrowIntColumnVector implements IntColumnVector {

    private final IntVector intVector;

    public ArrowIntColumnVector(IntVector intVector) {
        this.intVector = checkNotNull(intVector);
    }

    @Override
    public int getInt(int i) {
        return intVector.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return intVector.isNull(i);
    }
}
