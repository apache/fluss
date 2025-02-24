/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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
import com.alibaba.fluss.row.columnar.BytesColumnVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.FixedSizeBinaryVector;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Arrow column vector for Binary. */
@Internal
public class ArrowBinaryColumnVector implements BytesColumnVector {

    /** Container which is used to store the sequence of varbinary values of a column to read. */
    private final FixedSizeBinaryVector fixedSizeBinaryVector;

    public ArrowBinaryColumnVector(FixedSizeBinaryVector fixedSizeBinaryVector) {
        this.fixedSizeBinaryVector = checkNotNull(fixedSizeBinaryVector);
    }

    @Override
    public Bytes getBytes(int i) {
        byte[] bytes = fixedSizeBinaryVector.get(i);
        return new BytesColumnVector.Bytes(bytes, 0, bytes.length);
    }

    @Override
    public boolean isNullAt(int i) {
        return fixedSizeBinaryVector.isNull(i);
    }
}
