/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.row.array;

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.InternalRow;

/**
 * A {@link BinaryArray} that uses {@link org.apache.fluss.row.aligned.AlignedRow} as the binary
 * format for arrays of nested row type.
 */
public class AlignedArray extends BinaryArray {
    private static final long serialVersionUID = 1L;

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return BinarySegmentUtils.readAlignedRow(segments, offset, getLong(pos), numFields);
    }

    @Override
    protected BinaryArray createNestedArrayInstance() {
        return new AlignedArray();
    }
}
