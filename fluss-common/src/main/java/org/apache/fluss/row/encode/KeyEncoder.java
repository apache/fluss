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

package org.apache.fluss.row.encode;

import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.iceberg.IcebergKeyEncoder;
import org.apache.fluss.row.encode.paimon.PaimonKeyEncoder;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** An interface for encoding key of row into bytes. */
public interface KeyEncoder {

    /** Encode the key of given row to byte array. */
    byte[] encodeKey(InternalRow row);

    // -------------------------------------------------------------------------
    //  Factory Methods
    // -------------------------------------------------------------------------

    /**
     * Creates a primary key encoder for the given table configuration.
     *
     * <p><b>Backward Compatibility for legacy table (kvFormatVersion = 1):</b> For tables created
     * before the introduction of kv format version (legacy tables without kvFormatVersion), we
     * continue to use the original encoding method (lake's encoder for lake tables) to ensure data
     * compatibility.
     *
     * <p><b>New Tables (kvFormatVersion = 2):</b> For new tables, we cannot always use the lake's
     * encoder because some lake encoders (e.g., Paimon) don't support prefix lookup. Prefix lookup
     * requires the bucket key bytes encoded as a prefix of the primary key bytes encoded, which
     * Paimon's encoding format does not guarantee. To solve this, new tables use Fluss's own {@link
     * CompactedKeyEncoder} which ensures the bucket key bytes encoded is always a prefix of the
     * primary key bytes encoded.
     *
     * <p><b>Optimization for Default Bucket Key:</b> Prefix lookup is only needed when the bucket
     * key is a subset of the primary key. If {@code isDefaultBucketKey} is true (bucket key equals
     * primary key), prefix lookup is not supported, so we can directly use the lake's encoder. This
     * also provides a performance benefit: since bucket calculation always requires encoding the
     * bucket key using the lake's encoder (to ensure correct bucketing for the lake), when the
     * primary key uses the same lake's encoder, the encoded primary key bytes can be directly
     * reused for bucket calculation, saving one encoding operation.
     *
     * @param rowType the row type of the input row
     * @param keyFields the primary key fields to encode
     * @param tableConfig the table configuration containing kv format version and lake format
     * @param isDefaultBucketKey true if bucket key equals primary key (no prefix lookup supported)
     * @return the primary key encoder
     */
    static KeyEncoder ofPrimaryKeyEncoder(
            RowType rowType,
            List<String> keyFields,
            TableConfig tableConfig,
            boolean isDefaultBucketKey) {
        Optional<Integer> optKvFormatVersion = tableConfig.getKvFormatVersion();
        DataLakeFormat dataLakeFormat = tableConfig.getDataLakeFormat().orElse(null);
        int kvFormatVersion = optKvFormatVersion.orElse(1);
        if (kvFormatVersion == 1) {
            return of(rowType, keyFields, dataLakeFormat);
        }
        if (kvFormatVersion == 2) {
            if (isDefaultBucketKey) {
                return of(rowType, keyFields, dataLakeFormat);
            } else {
                // use CompactedKeyEncoder to support prefix look up
                return CompactedKeyEncoder.createKeyEncoder(rowType, keyFields);
            }
        }
        throw new UnsupportedOperationException(
                "Unsupported kv format version: " + kvFormatVersion);
    }

    /**
     * Creates a bucket key encoder for bucket calculation with a custom row type.
     *
     * @param rowType the row type of the input row
     * @param keyFields the bucket key fields to encode
     * @param lakeFormat the datalake format
     * @return the bucket key encoder
     */
    static KeyEncoder ofBucketKeyEncoder(
            RowType rowType, List<String> keyFields, @Nullable DataLakeFormat lakeFormat) {
        return of(rowType, keyFields, lakeFormat);
    }

    /**
     * Creates a key encoder based on the datalake format.
     *
     * <p>Internal use only. This method should be private, but Java 8 doesn't support private
     * static methods in interfaces. Please use {@link #ofPrimaryKeyEncoder} or {@link
     * #ofBucketKeyEncoder} instead.
     *
     * @param rowType the row type of the input row
     * @param keyFields the key fields to encode
     * @param lakeFormat the datalake format, null means no datalake
     * @deprecated This is an internal method and should not be used externally.
     */
    @Deprecated
    static KeyEncoder of(
            RowType rowType, List<String> keyFields, @Nullable DataLakeFormat lakeFormat) {
        if (lakeFormat == null) {
            // use default compacted key encoder
            return CompactedKeyEncoder.createKeyEncoder(rowType, keyFields);
        } else if (lakeFormat == DataLakeFormat.PAIMON) {
            return new PaimonKeyEncoder(rowType, keyFields);
        } else if (lakeFormat == DataLakeFormat.LANCE) {
            // use default compacted key encoder
            return CompactedKeyEncoder.createKeyEncoder(rowType, keyFields);
        } else if (lakeFormat == DataLakeFormat.ICEBERG) {
            return new IcebergKeyEncoder(rowType, keyFields);
        } else {
            throw new UnsupportedOperationException("Unsupported datalake format: " + lakeFormat);
        }
    }
}
