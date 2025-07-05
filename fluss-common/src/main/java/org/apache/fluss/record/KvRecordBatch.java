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

package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.decode.RowDecoder;

/**
 * A kv record batch is a container for {@link KvRecord KvRecords}.
 *
 * @since 0.1
 */
@PublicEvolving
public interface KvRecordBatch {

    /** The "magic" values. */
    byte KV_MAGIC_VALUE_V0 = 0;

    /** The current "magic" value. */
    byte CURRENT_KV_MAGIC_VALUE = KV_MAGIC_VALUE_V0;

    /**
     * Check whether the checksum of this batch is correct.
     *
     * @return true If so, false otherwise
     */
    boolean isValid();

    /** Raise an exception if the checksum is not valid. */
    void ensureValid();

    /**
     * Get the checksum of this record batch, which covers the batch header as well as all of the
     * records.
     *
     * @return The 4-byte unsigned checksum represented as a long
     */
    long checksum();

    /**
     * Get the schema id of this record batch.
     *
     * @return The schema id
     */
    short schemaId();

    /**
     * Get the record format version of this record batch (i.e its magic value).
     *
     * @return the magic byte
     */
    byte magic();

    /**
     * Get writer id for this log record batch.
     *
     * @return writer id
     */
    long writerId();

    /**
     * Get batch base sequence for this log record batch. the base sequence is the first sequence
     * number of this batch, it's used to protect the idempotence of the those batches write by same
     * writer.
     *
     * @return batch base sequence
     */
    int batchSequence();

    /**
     * Get the size in bytes of this batch, including the size of the record and the batch overhead.
     *
     * @return The size in bytes of this batch
     */
    int sizeInBytes();

    /**
     * Get the count.
     *
     * @return The number of records in the batch.
     */
    int getRecordCount();

    /**
     * Get the iterable of {@link KvRecord} in this batch.
     *
     * @param readContext The context to read records from the record batch
     * @return The iterable of {@link KvRecord} in this batch
     */
    Iterable<KvRecord> records(ReadContext readContext);

    /** The read context of a {@link KvRecordBatch} to read records. */
    interface ReadContext {

        /**
         * Gets the row decoder for the given schema to decode bytes read from {@link
         * KvRecordBatch}.
         *
         * @param schemaId the schema of the kv records
         */
        RowDecoder getRowDecoder(int schemaId);
    }
}
