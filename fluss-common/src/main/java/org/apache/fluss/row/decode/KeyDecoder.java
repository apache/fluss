/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.decode;

import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Interface for decoding key bytes back to {@link InternalRow}.
 *
 * <p>This interface provides functionality to decode binary key bytes into internal row
 * representation, typically used for primary key decoding in KV tables.
 */
public interface KeyDecoder {
    /** Decode key bytes to a row containing only key fields, without non-key fields. */
    InternalRow decodeKey(byte[] keyBytes);

    static KeyDecoder ofPrimaryKeyDecoder(
            RowType rowType,
            List<String> keyFields,
            short kvFormatVersion,
            @Nullable DataLakeFormat lakeFormat,
            boolean isDefaultBucketKey) {
        if (kvFormatVersion == 1 || (kvFormatVersion == 2 && isDefaultBucketKey)) {
            if (lakeFormat == null || lakeFormat == DataLakeFormat.LANCE) {
                return CompactedKeyDecoder.createKeyDecoder(rowType, keyFields);
            }
            if (lakeFormat == DataLakeFormat.PAIMON) {
                // TODO: Implement key decoding support for Paimon lake format
                throw new UnsupportedOperationException(
                        "Paimon lake format does not support key decoding");
            }
            if (lakeFormat == DataLakeFormat.ICEBERG) {
                // TODO: Implement key decoding support for Iceberg lake format
                throw new UnsupportedOperationException(
                        "Iceberg lake format does not support key decoding");
            }
            throw new UnsupportedOperationException(
                    "Unsupported datalake format for key decoding: " + lakeFormat);
        }
        if (kvFormatVersion == 2) {
            // use CompactedKeyEncoder to support prefix look up
            return CompactedKeyDecoder.createKeyDecoder(rowType, keyFields);
        }
        throw new UnsupportedOperationException(
                "Unsupported kv format version: " + kvFormatVersion);
    }
}
