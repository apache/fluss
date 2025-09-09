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

import org.apache.fluss.exception.OutOfOrderSequenceException;

/** The format of Fluss how to organize and storage a {@link LogRecordBatch}. */
public class LogRecordBatchFormat {

    // ----------------------------------------------------------------------------------------
    // Common Variables
    // ----------------------------------------------------------------------------------------

    /** Value used if non-idempotent. */
    public static final long NO_WRITER_ID = -1L;

    public static final int NO_BATCH_SEQUENCE = -1;

    /**
     * Used to indicate an unknown leaderEpoch, which will be the case when the record set is first
     * created by the writer or the magic lower than V1.
     */
    public static final int NO_LEADER_EPOCH = -1;

    public static final int BASE_OFFSET_LENGTH = 8;
    public static final int LENGTH_LENGTH = 4;
    public static final int MAGIC_LENGTH = 1;
    private static final int COMMIT_TIMESTAMP_LENGTH = 8;
    private static final int CRC_LENGTH = 4;
    private static final int SCHEMA_ID_LENGTH = 2;
    private static final int LEADER_EPOCH_LENGTH = 4;
    private static final int ATTRIBUTE_LENGTH = 1;
    private static final int LAST_OFFSET_DELTA_LENGTH = 4;
    private static final int WRITE_CLIENT_ID_LENGTH = 8;
    private static final int BATCH_SEQUENCE_LENGTH = 4;
    private static final int RECORDS_COUNT_LENGTH = 4;

    public static final int BASE_OFFSET_OFFSET = 0;
    public static final int LENGTH_OFFSET = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
    public static final int MAGIC_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
    public static final int COMMIT_TIMESTAMP_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final int LOG_OVERHEAD = LENGTH_OFFSET + LENGTH_LENGTH;
    public static final int HEADER_SIZE_UP_TO_MAGIC = MAGIC_OFFSET + MAGIC_LENGTH;

    // ----------------------------------------------------------------------------------------
    // Format of Magic Version: V1
    // ----------------------------------------------------------------------------------------

    /**
     * LogRecordBatch implementation for magic 1 (V1). The schema of {@link LogRecordBatch} is given
     * below:
     *
     * <ul>
     *   RecordBatch =>
     *   <li>BaseOffset => Int64
     *   <li>Length => Int32
     *   <li>Magic => Int8
     *   <li>CommitTimestamp => Int64
     *   <li>LeaderEpoch => Int32
     *   <li>CRC => Uint32
     *   <li>SchemaId => Int16
     *   <li>Attributes => Int8
     *   <li>LastOffsetDelta => Int32
     *   <li>WriterID => Int64
     *   <li>SequenceID => Int32
     *   <li>RecordCount => Int32
     *   <li>Records => [Record]
     * </ul>
     *
     * <p>Newly added field in LogRecordBatch header of magic V1 is LeaderEpoch, which used to build
     * a consistent leaderEpoch cache across different tabletServers.
     *
     * <p>The CRC covers the data from the schemaId to the end of the batch (i.e. all the bytes that
     * follow the CRC). It is located after the magic byte, which means that clients must parse the
     * magic byte before deciding how to interpret the bytes between the batch length and the magic
     * byte. The CRC-32C (Castagnoli) polynomial is used for the computation. CommitTimestamp is
     * also located before the CRC, because it is determined in server side.
     *
     * <p>The field 'lastOffsetDelta is used to calculate the lastOffset of the current batch as:
     * [lastOffset = baseOffset + LastOffsetDelta] instead of [lastOffset = baseOffset + recordCount
     * - 1]. The reason for introducing this field is that there might be cases where the offset
     * delta in batch does not match the recordCount. For example, when generating CDC logs for a kv
     * table and sending a batch that only contains the deletion of non-existent kvs, no CDC logs
     * would be generated. However, we need to increment the batchSequence for the corresponding
     * writerId to make sure no {@link OutOfOrderSequenceException} will be thrown. In such a case,
     * we would generate a logRecordBatch with a LastOffsetDelta of 0 but a recordCount of 0.
     *
     * <p>The current attributes are given below:
     *
     * <pre>
     * ------------------------------------------
     * |  Unused (1-7)   |  AppendOnly Flag (0) |
     * ------------------------------------------
     * </pre>
     *
     * @since 0.7
     */
    public static final byte LOG_MAGIC_VALUE_V1 = 1;

    private static final int V1_LEADER_EPOCH_OFFSET =
            COMMIT_TIMESTAMP_OFFSET + COMMIT_TIMESTAMP_LENGTH;
    private static final int V1_CRC_OFFSET = V1_LEADER_EPOCH_OFFSET + LEADER_EPOCH_LENGTH;
    private static final int V1_SCHEMA_ID_OFFSET = V1_CRC_OFFSET + CRC_LENGTH;
    private static final int V1_ATTRIBUTES_OFFSET = V1_SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
    private static final int V1_LAST_OFFSET_DELTA_OFFSET = V1_ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    private static final int V1_WRITE_CLIENT_ID_OFFSET =
            V1_LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
    private static final int V1_BATCH_SEQUENCE_OFFSET =
            V1_WRITE_CLIENT_ID_OFFSET + WRITE_CLIENT_ID_LENGTH;
    private static final int V1_RECORDS_COUNT_OFFSET =
            V1_BATCH_SEQUENCE_OFFSET + BATCH_SEQUENCE_LENGTH;
    private static final int V1_RECORDS_OFFSET = V1_RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;

    public static final int V1_RECORD_BATCH_HEADER_SIZE = V1_RECORDS_OFFSET;
    private static final int V1_ARROW_CHANGETYPE_OFFSET = V1_RECORD_BATCH_HEADER_SIZE;

    // ----------------------------------------------------------------------------------------
    // Format of Magic Version: V0
    // ----------------------------------------------------------------------------------------

    /**
     * LogRecordBatch implementation for magic 0 (V0). The schema of {@link LogRecordBatch} is given
     * below:
     *
     * <ul>
     *   RecordBatch =>
     *   <li>BaseOffset => Int64
     *   <li>Length => Int32
     *   <li>Magic => Int8
     *   <li>CommitTimestamp => Int64
     *   <li>CRC => Uint32
     *   <li>SchemaId => Int16
     *   <li>Attributes => Int8
     *   <li>LastOffsetDelta => Int32
     *   <li>WriterID => Int64
     *   <li>SequenceID => Int32
     *   <li>RecordCount => Int32
     *   <li>Records => [Record]
     * </ul>
     *
     * <p>The current attributes are given below:
     *
     * <pre>
     * ------------------------------------------
     * |  Unused (1-7)   |  AppendOnly Flag (0) |
     * ------------------------------------------
     * </pre>
     *
     * @since 0.1
     */
    public static final byte LOG_MAGIC_VALUE_V0 = 0;

    private static final int V0_CRC_OFFSET = COMMIT_TIMESTAMP_OFFSET + COMMIT_TIMESTAMP_LENGTH;
    private static final int V0_SCHEMA_ID_OFFSET = V0_CRC_OFFSET + CRC_LENGTH;
    private static final int V0_ATTRIBUTES_OFFSET = V0_SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
    private static final int V0_LAST_OFFSET_DELTA_OFFSET = V0_ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    private static final int V0_WRITE_CLIENT_ID_OFFSET =
            V0_LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
    private static final int V0_BATCH_SEQUENCE_OFFSET =
            V0_WRITE_CLIENT_ID_OFFSET + WRITE_CLIENT_ID_LENGTH;
    private static final int V0_RECORDS_COUNT_OFFSET =
            V0_BATCH_SEQUENCE_OFFSET + BATCH_SEQUENCE_LENGTH;
    private static final int V0_RECORDS_OFFSET = V0_RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;

    public static final int V0_RECORD_BATCH_HEADER_SIZE = V0_RECORDS_OFFSET;
    private static final int V0_ARROW_CHANGETYPE_OFFSET = V0_RECORD_BATCH_HEADER_SIZE;

    // ----------------------------------------------------------------------------------------
    // Static Methods
    // ----------------------------------------------------------------------------------------

    public static int leaderEpochOffset(byte magic) {
        if (magic == LOG_MAGIC_VALUE_V1) {
            return V1_LEADER_EPOCH_OFFSET;
        }
        throw new IllegalArgumentException("Unsupported magic value " + magic);
    }

    public static int crcOffset(byte magic) {
        switch (magic) {
            case LOG_MAGIC_VALUE_V1:
                return V1_CRC_OFFSET;
            case LOG_MAGIC_VALUE_V0:
                return V0_CRC_OFFSET;
            default:
                throw new IllegalArgumentException("Unsupported magic value " + magic);
        }
    }

    public static int schemaIdOffset(byte magic) {
        switch (magic) {
            case LOG_MAGIC_VALUE_V1:
                return V1_SCHEMA_ID_OFFSET;
            case LOG_MAGIC_VALUE_V0:
                return V0_SCHEMA_ID_OFFSET;
            default:
                throw new IllegalArgumentException("Unsupported magic value " + magic);
        }
    }

    public static int attributeOffset(byte magic) {
        switch (magic) {
            case LOG_MAGIC_VALUE_V1:
                return V1_ATTRIBUTES_OFFSET;
            case LOG_MAGIC_VALUE_V0:
                return V0_ATTRIBUTES_OFFSET;
            default:
                throw new IllegalArgumentException("Unsupported magic value " + magic);
        }
    }

    public static int lastOffsetDeltaOffset(byte magic) {
        switch (magic) {
            case LOG_MAGIC_VALUE_V1:
                return V1_LAST_OFFSET_DELTA_OFFSET;
            case LOG_MAGIC_VALUE_V0:
                return V0_LAST_OFFSET_DELTA_OFFSET;
            default:
                throw new IllegalArgumentException("Unsupported magic value " + magic);
        }
    }

    public static int writeClientIdOffset(byte magic) {
        switch (magic) {
            case LOG_MAGIC_VALUE_V1:
                return V1_WRITE_CLIENT_ID_OFFSET;
            case LOG_MAGIC_VALUE_V0:
                return V0_WRITE_CLIENT_ID_OFFSET;
            default:
                throw new IllegalArgumentException("Unsupported magic value " + magic);
        }
    }

    public static int batchSequenceOffset(byte magic) {
        switch (magic) {
            case LOG_MAGIC_VALUE_V1:
                return V1_BATCH_SEQUENCE_OFFSET;
            case LOG_MAGIC_VALUE_V0:
                return V0_BATCH_SEQUENCE_OFFSET;
            default:
                throw new IllegalArgumentException("Unsupported magic value " + magic);
        }
    }

    public static int recordsCountOffset(byte magic) {
        switch (magic) {
            case LOG_MAGIC_VALUE_V1:
                return V1_RECORDS_COUNT_OFFSET;
            case LOG_MAGIC_VALUE_V0:
                return V0_RECORDS_COUNT_OFFSET;
            default:
                throw new IllegalArgumentException("Unsupported magic value " + magic);
        }
    }

    public static int recordBatchHeaderSize(byte magic) {
        switch (magic) {
            case LOG_MAGIC_VALUE_V1:
                return V1_RECORD_BATCH_HEADER_SIZE;
            case LOG_MAGIC_VALUE_V0:
                return V0_RECORD_BATCH_HEADER_SIZE;
            default:
                throw new IllegalArgumentException("Unsupported magic value " + magic);
        }
    }

    public static int arrowChangeTypeOffset(byte magic) {
        switch (magic) {
            case LOG_MAGIC_VALUE_V1:
                return V1_ARROW_CHANGETYPE_OFFSET;
            case LOG_MAGIC_VALUE_V0:
                return V0_ARROW_CHANGETYPE_OFFSET;
            default:
                throw new IllegalArgumentException("Unsupported magic value " + magic);
        }
    }
}
