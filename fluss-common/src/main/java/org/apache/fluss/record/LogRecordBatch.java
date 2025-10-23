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
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import java.util.Iterator;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;

/**
 * A record batch is a container for {@link LogRecord LogRecords}.
 *
 * @since 0.1
 */
@PublicEvolving
public interface LogRecordBatch {
    /**
     * The current "magic" value. Even though we already support LOG_MAGIC_VALUE_V1, for
     * compatibility reasons — specifically, a higher-version Fluss Client (which supports
     * LOG_MAGIC_VALUE_V1) cannot write to a lower-version Fluss Server (which only supports
     * LOG_MAGIC_VALUE_V0) — we are unable to guarantee compatibility at this time. Therefore, we
     * will keep the current log magic value set to LOG_MAGIC_VALUE_V0 for now, and only upgrade it
     * to LOG_MAGIC_VALUE_V1 once the compatibility issue is resolved.
     */
    byte CURRENT_LOG_MAGIC_VALUE = LOG_MAGIC_VALUE_V0;

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
     * Get the base log offset contained in this record batch.
     *
     * @return The base offset of this record batch (which may or may not be the offset of the first
     *     record as described above).
     */
    long baseLogOffset();

    /**
     * Get the last log offset in this record batch (inclusive). Just like {@link #baseLogOffset()},
     * the last offset always reflects the offset of the last record in the original batch.
     *
     * @return The offset of the last record in this batch
     */
    long lastLogOffset();

    /**
     * Get the log offset following this record batch (i.e. the last offset contained in this batch
     * plus one).
     *
     * @return the next consecutive offset following this batch
     */
    long nextLogOffset();

    /**
     * Get the record format version of this record batch (i.e its magic value).
     *
     * @return the magic byte
     */
    byte magic();

    /**
     * Get commit timestamp of this record batch. Commit timestamp means the timestamp when the
     * batch is appended to the log segment in server.
     *
     * @return the commit timestamp
     */
    long commitTimestamp();

    /**
     * Get writer id for this log record batch.
     *
     * @return writer id
     */
    long writerId();

    /** Does the batch have a valid writer id set. */
    default boolean hasWriterId() {
        return writerId() != NO_WRITER_ID;
    }

    /**
     * Get batch sequence number for this log record batch. it's used to protect the idempotence of
     * the written batches write by same writer.
     *
     * @return batch base sequence
     */
    int batchSequence();

    /**
     * Get leader epoch of this bucket for this log record batch.
     *
     * @return leader epoch
     */
    int leaderEpoch();

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
     * Returns a closeable iterator of records for this batch which basically delays deserialization
     * of the record stream until the records are actually asked for using {@link Iterator#next()}.
     * Callers should ensure that the iterator is closed.
     *
     * @param context The context to read records from the record batch.
     * @return The closeable iterator of records in this batch
     * @see ReadContext
     */
    CloseableIterator<LogRecord> records(ReadContext context);

    /** The read context of a {@link LogRecordBatch} to read records. */
    interface ReadContext {

        /** Gets the log format of the record batch. */
        LogFormat getLogFormat();

        /**
         * Get the row type of the schema id. The returned row type is projected if the record batch
         * is a projected {@link LogRecordBatch}.
         *
         * @param schemaId The schema id of the record batch.
         * @return The (maybe projected) row type of the record batch.
         */
        RowType getRowType(int schemaId);

        /**
         * Gets the Arrow {@link VectorSchemaRoot} for the given schema id. The returned schema root
         * is projected if the record batch is a projected {@link LogRecordBatch}.
         *
         * <p>The schema root is used to read the Arrow records in the batch, if this is a {@link
         * LogFormat#ARROW} record batch.
         *
         * <p>Note: DO NOT close the vector schema root because it is shared across multiple
         * batches. Use {@link VectorSchemaRoot#slice(int)} to cache the root and close it after
         * use.
         *
         * @param schemaId The schema id of the record batch.
         * @return The (maybe projected) schema root of the record batch.
         */
        VectorSchemaRoot getVectorSchemaRoot(int schemaId);

        /** Gets the buffer allocator. */
        BufferAllocator getBufferAllocator();
    }
}
