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

package com.alibaba.fluss.row.encode;

import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.paimon.PaimonKeyEncoder;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** An interface for encoding key of row into bytes. */
public interface KeyEncoder {

    /** Encode the key of given row to byte array. */
    byte[] encodeKey(InternalRow row);

    /**
     * Create a key encoder to encode the key array bytes of the input row.
     *
     * @param rowType the row type of the input row
     * @param keyFields the key fields to encode
     * @param lakeFormat the datalake format
     */
    static KeyEncoder of(
            RowType rowType, List<String> keyFields, @Nullable DataLakeFormat lakeFormat) {
        if (lakeFormat == null) {
            // use default compacted key encoder
            return CompactedKeyEncoder.createKeyEncoder(rowType, keyFields);
        } else if (lakeFormat == DataLakeFormat.PAIMON) {
            return new PaimonKeyEncoder(rowType, keyFields);
        } else {
            throw new UnsupportedOperationException("Unsupported datalake format: " + lakeFormat);
        }
    }
}
