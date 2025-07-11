/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.kafka.utils;

import com.alibaba.fluss.metadata.ValidationException;

import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;

import static com.alibaba.fluss.record.DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK;

/** Parser for Kafka MemoryRecords. */
public class MemoryRecordsParser {
    public static final int BASE_OFFSET_OFFSET = 0; // 8 bytes
    public static final int LENGTH_OFFSET = BASE_OFFSET_OFFSET + 8; // 4 bytes
    public static final int PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + 4; // 4 bytes
    public static final int MAGIC_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + 4; // 1 byte
    public static final int CRC_OFFSET = MAGIC_OFFSET + 1; // 4 bytes
    public static final int ATTRIBUTES_OFFSET = CRC_OFFSET + 4; // 2 bytes
    public static final int LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + 2; // 4 bytes
    public static final int BASE_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + 4; // 8 bytes
    public static final int MAX_TIMESTAMP_OFFSET = BASE_TIMESTAMP_OFFSET + 8; // 8 bytes
    public static final int PRODUCER_ID_OFFSET = MAX_TIMESTAMP_OFFSET + 8; // 8 bytes
    public static final int PRODUCER_EPOCH_OFFSET = PRODUCER_ID_OFFSET + 8; // 2 bytes
    public static final int BASE_SEQUENCE_OFFSET = PRODUCER_EPOCH_OFFSET + 2; // 4 bytes
    public static final int RECORDS_COUNT_OFFSET = BASE_SEQUENCE_OFFSET + 4; // 4 bytes
    public static final int RECORDS_OFFSET_OVERHEAD = RECORDS_COUNT_OFFSET + 4; // start data

    /**
     * Extract the body of the Kafka records.
     *
     * @param buffer
     * @return
     */
    public static ByteBuffer extractBody(ByteBuffer buffer) {
        validate(buffer);
        int batchLength = buffer.getInt(LENGTH_OFFSET) + Records.LOG_OVERHEAD;
        if (buffer.remaining() < batchLength) {
            throw new ValidationException("Invalid Kafka batch size");
        }
        int dataLength = batchLength - RECORDS_OFFSET_OVERHEAD;
        ByteBuffer bodySlice = buffer.duplicate();
        bodySlice.position(RECORDS_OFFSET_OVERHEAD);
        bodySlice.limit(RECORDS_OFFSET_OVERHEAD + dataLength);

        return bodySlice.slice();
    }

    /**
     * Extract the header of the Kafka records.
     *
     * @param buffer
     * @return
     */
    public static Header extractHeader(ByteBuffer buffer) {
        validate(buffer);
        if (buffer.get(MAGIC_OFFSET) != RecordBatch.MAGIC_VALUE_V2) {
            throw new ValidationException("Only support Kafka V2 records");
        }
        return new Header.Builder()
                .baseOffset(buffer.getLong(BASE_OFFSET_OFFSET))
                .length(buffer.getInt(LENGTH_OFFSET))
                .partitionLeaderEpoch(buffer.getInt(PARTITION_LEADER_EPOCH_OFFSET))
                .magic(buffer.get(MAGIC_OFFSET))
                .crc(buffer.getInt(CRC_OFFSET))
                .attributes(buffer.getShort(ATTRIBUTES_OFFSET))
                .lastOffsetDelta(buffer.getInt(LAST_OFFSET_DELTA_OFFSET))
                .baseTimestamp(buffer.getLong(BASE_TIMESTAMP_OFFSET))
                .maxTimestamp(buffer.getLong(MAX_TIMESTAMP_OFFSET))
                .producerId(buffer.getLong(PRODUCER_ID_OFFSET))
                .producerEpoch(buffer.getShort(PRODUCER_EPOCH_OFFSET))
                .baseSequence(buffer.getInt(BASE_SEQUENCE_OFFSET))
                .recordsCount(buffer.getInt(RECORDS_COUNT_OFFSET))
                .build();
    }

    private static void validate(ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < RECORDS_OFFSET_OVERHEAD) {
            throw new ValidationException("Invalid input buffer or insufficient data");
        }
    }

    /**
     * kafka attributes.
     *
     * <p>---------------------------------------------------------------------------------------------------------------------------
     * | Unused (7-15) | Delete Horizon Flag (6) | Control (5) | Transactional (4) | Timestamp Type
     * (3) | Compression Type (0-2) |
     * ---------------------------------------------------------------------------------------------------------------------------
     *
     * @param kafkaAttributes Kafka attributes to parse
     * @param appendOnly Flag indicating if the batch is append-only
     * @return Fluss attributes
     */
    public static short computeFlussAttributes(short kafkaAttributes, boolean appendOnly) {
        short flussAttributes = (short) ((kafkaAttributes & 0x7F) << 1); // 0-6 bit
        if (appendOnly) {
            flussAttributes |= APPEND_ONLY_FLAG_MASK; // set append only flag
        }
        return flussAttributes;
    }

    public static short recoverKafkaAttributes(short flussAttributes) {
        short highBits = (short) ((flussAttributes & 0xF0) >>> 1);
        short lowBits = (short) ((flussAttributes & 0x0E) >>> 1);
        return (short) (highBits | lowBits);
    }

    /** Kafka batch record header. */
    public static class Header {
        private final long baseOffset;
        private final int length;
        private final int partitionLeaderEpoch;
        private final byte magic;
        private final int crc;
        private final short attributes;
        private final int lastOffsetDelta;
        private final long baseTimestamp;
        private final long maxTimestamp;
        private final long producerId;
        private final short producerEpoch;
        private final int baseSequence;
        private final int recordsCount;

        private Header(Builder builder) {
            this.baseOffset = builder.baseOffset;
            this.length = builder.length;
            this.partitionLeaderEpoch = builder.partitionLeaderEpoch;
            this.magic = builder.magic;
            this.crc = builder.crc;
            this.attributes = builder.attributes;
            this.lastOffsetDelta = builder.lastOffsetDelta;
            this.baseTimestamp = builder.baseTimestamp;
            this.maxTimestamp = builder.maxTimestamp;
            this.producerId = builder.producerId;
            this.producerEpoch = builder.producerEpoch;
            this.baseSequence = builder.baseSequence;
            this.recordsCount = builder.recordsCount;
        }

        public long getBaseOffset() {
            return baseOffset;
        }

        public int getLength() {
            return length;
        }

        public int getPartitionLeaderEpoch() {
            return partitionLeaderEpoch;
        }

        public byte getMagic() {
            return magic;
        }

        public int getCrc() {
            return crc;
        }

        public short getAttributes() {
            return attributes;
        }

        public int getLastOffsetDelta() {
            return lastOffsetDelta;
        }

        public long getBaseTimestamp() {
            return baseTimestamp;
        }

        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        public long getProducerId() {
            return producerId;
        }

        public short getProducerEpoch() {
            return producerEpoch;
        }

        public int getBaseSequence() {
            return baseSequence;
        }

        public int getRecordsCount() {
            return recordsCount;
        }

        @Override
        public String toString() {
            return "KafkaHeader{"
                    + "baseOffset="
                    + baseOffset
                    + ", length="
                    + length
                    + ", partitionLeaderEpoch="
                    + partitionLeaderEpoch
                    + ", magic="
                    + magic
                    + ", crc="
                    + crc
                    + ", attributes="
                    + attributes
                    + ", lastOffsetDelta="
                    + lastOffsetDelta
                    + ", baseTimestamp="
                    + baseTimestamp
                    + ", maxTimestamp="
                    + maxTimestamp
                    + ", producerId="
                    + producerId
                    + ", producerEpoch="
                    + producerEpoch
                    + ", baseSequence="
                    + baseSequence
                    + ", recordsCount="
                    + recordsCount
                    + '}';
        }

        /** Kafka batch record header builder. */
        public static class Builder {
            private long baseOffset;
            private int length;
            private int partitionLeaderEpoch;
            private byte magic;
            private int crc;
            private short attributes;
            private int lastOffsetDelta;
            private long baseTimestamp;
            private long maxTimestamp;
            private long producerId;
            private short producerEpoch;
            private int baseSequence;
            private int recordsCount;

            public Builder baseOffset(long baseOffset) {
                this.baseOffset = baseOffset;
                return this;
            }

            public Builder length(int length) {
                this.length = length;
                return this;
            }

            public Builder partitionLeaderEpoch(int partitionLeaderEpoch) {
                this.partitionLeaderEpoch = partitionLeaderEpoch;
                return this;
            }

            public Builder magic(byte magic) {
                this.magic = magic;
                return this;
            }

            public Builder crc(int crc) {
                this.crc = crc;
                return this;
            }

            public Builder attributes(short attributes) {
                this.attributes = attributes;
                return this;
            }

            public Builder lastOffsetDelta(int lastOffsetDelta) {
                this.lastOffsetDelta = lastOffsetDelta;
                return this;
            }

            public Builder baseTimestamp(long baseTimestamp) {
                this.baseTimestamp = baseTimestamp;
                return this;
            }

            public Builder maxTimestamp(long maxTimestamp) {
                this.maxTimestamp = maxTimestamp;
                return this;
            }

            public Builder producerId(long producerId) {
                this.producerId = producerId;
                return this;
            }

            public Builder producerEpoch(short producerEpoch) {
                this.producerEpoch = producerEpoch;
                return this;
            }

            public Builder baseSequence(int baseSequence) {
                this.baseSequence = baseSequence;
                return this;
            }

            public Builder recordsCount(int recordsCount) {
                this.recordsCount = recordsCount;
                return this;
            }

            public Header build() {
                return new Header(this);
            }
        }
    }
}
