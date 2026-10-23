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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Write-side specification for one secondary index: column mapping, encoders, bucketing. */
@Internal
final class IndexSpec {

    /**
     * Encodes one complete physical Index Table entry from a base row. Encoders may reuse storage
     * for entry values between calls, but each returned key and target bucket must remain stable.
     */
    @FunctionalInterface
    interface EntryEncoder {
        IndexEntry encode(InternalRow row);
    }

    /** Encodes the durable progress record for one target bucket. */
    @FunctionalInterface
    interface ProgressEncoder {
        IndexEntry encode(TableBucket sourceBucket, int targetBucket, long sourceEndOffset);
    }

    /**
     * One encoded physical Index Table entry. The key and target bucket remain stable. The value
     * may be backed by a reusable row encoder and is valid only until the next {@link
     * IndexSpec#encodeEntry(InternalRow)} call on the same spec.
     */
    static final class IndexEntry {
        private final byte[] key;
        private final BinaryRow value;
        private final int targetBucket;

        IndexEntry(byte[] key, BinaryRow value, int targetBucket) {
            this.key = checkNotNull(key, "key");
            this.value = checkNotNull(value, "value");
            this.targetBucket = targetBucket;
        }

        byte[] key() {
            return key;
        }

        /** Returns the encoder-backed value, which must be consumed before the next encode call. */
        BinaryRow value() {
            return value;
        }

        int targetBucket() {
            return targetBucket;
        }
    }

    private final long indexTableId;
    private final String indexName;
    private final IndexVisibility visibility;
    private final int indexSchemaId;
    private final KvFormat indexKvFormat;
    private final int[] idxColumnIndices;
    private final EntryEncoder entryEncoder;
    private final ProgressEncoder progressEncoder;

    IndexSpec(
            String indexName,
            IndexVisibility visibility,
            long indexTableId,
            int indexSchemaId,
            KvFormat indexKvFormat,
            int[] idxColumnIndices,
            EntryEncoder entryEncoder,
            ProgressEncoder progressEncoder) {
        this.indexName = checkNotNull(indexName, "indexName");
        this.visibility = checkNotNull(visibility, "visibility");
        this.indexTableId = indexTableId;
        this.indexSchemaId = indexSchemaId;
        this.indexKvFormat = indexKvFormat;
        this.idxColumnIndices = checkNotNull(idxColumnIndices, "idxColumnIndices").clone();
        checkArgument(idxColumnIndices.length > 0, "idxColumnIndices must not be empty.");
        this.entryEncoder = checkNotNull(entryEncoder, "entryEncoder");
        this.progressEncoder = checkNotNull(progressEncoder, "progressEncoder");
    }

    long getIndexTableId() {
        return indexTableId;
    }

    String getIndexName() {
        return indexName;
    }

    IndexVisibility getVisibility() {
        return visibility;
    }

    boolean isSync() {
        return visibility == IndexVisibility.SYNC;
    }

    int getIndexSchemaId() {
        return indexSchemaId;
    }

    KvFormat getIndexKvFormat() {
        return indexKvFormat;
    }

    int[] getIdxColumnIndices() {
        return idxColumnIndices.clone();
    }

    IndexEntry encodeEntry(InternalRow row) {
        return entryEncoder.encode(row);
    }

    IndexEntry encodeProgress(TableBucket sourceBucket, int targetBucket, long sourceEndOffset) {
        return progressEncoder.encode(sourceBucket, targetBucket, sourceEndOffset);
    }

    boolean hasIndexColumns(InternalRow row) {
        for (int idxColumnIndex : idxColumnIndices) {
            if (row.isNullAt(idxColumnIndex)) {
                return false;
            }
        }
        return true;
    }
}
