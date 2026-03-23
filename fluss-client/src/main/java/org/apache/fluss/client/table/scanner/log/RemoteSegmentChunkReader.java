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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.record.MemoryLogRecords;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_LENGTH;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;

/**
 * Reads a remote log segment file in chunks via {@link FSDataInputStream}. Each call to {@link
 * #readNextChunk(int)} reads up to {@code maxChunkSize} bytes, finds the last complete batch
 * boundary, and returns a {@link MemoryLogRecords} containing only complete batches.
 *
 * <p>If a single batch is larger than the chunk size, the entire batch is read in one go.
 */
@Internal
class RemoteSegmentChunkReader implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteSegmentChunkReader.class);

    private final FSDataInputStream inputStream;
    private long currentPosition;
    private final long endPosition;

    RemoteSegmentChunkReader(FSDataInputStream inputStream, long startPosition, long endPosition) {
        this.inputStream = inputStream;
        this.currentPosition = startPosition;
        this.endPosition = endPosition;
    }

    /**
     * Reads the next chunk of data from the remote file, truncating to complete batch boundaries.
     *
     * @param maxChunkSize the maximum number of bytes to read in one chunk
     * @return a {@link MemoryLogRecords} containing complete batches, or {@link
     *     MemoryLogRecords#EMPTY} if no complete batch could be read
     */
    MemoryLogRecords readNextChunk(int maxChunkSize) throws IOException {
        if (isExhausted()) {
            return MemoryLogRecords.EMPTY;
        }

        long remaining = endPosition - currentPosition;
        int bytesToRead = (int) Math.min(maxChunkSize, remaining);

        // Seek to the current position and read bytes.
        inputStream.seek(currentPosition);
        byte[] buffer = new byte[bytesToRead];
        int totalRead = readFully(buffer, 0, bytesToRead);

        if (totalRead == 0) {
            // No data read, mark as exhausted.
            currentPosition = endPosition;
            return MemoryLogRecords.EMPTY;
        }

        // Find the last complete batch boundary.
        int validBytes = findLastCompleteBatchEnd(buffer, totalRead);

        if (validBytes == 0) {
            // No complete batch in the chunk. This means a single batch is larger than
            // maxChunkSize. Read the entire oversized batch.
            return readOversizedBatch(buffer, totalRead);
        }

        currentPosition += validBytes;
        return MemoryLogRecords.pointToBytes(buffer, 0, validBytes);
    }

    /**
     * Scans through the buffer to find the end position of the last complete batch.
     *
     * @return the number of bytes up to and including the last complete batch, or 0 if no complete
     *     batch fits in the buffer
     */
    private int findLastCompleteBatchEnd(byte[] buffer, int bufferLength) {
        int position = 0;
        int lastCompleteBatchEnd = 0;

        while (position + LOG_OVERHEAD <= bufferLength) {
            // Read the batch length from the LENGTH_OFFSET within this batch.
            // Fluss uses little-endian byte order for the batch header.
            int batchLength =
                    ByteBuffer.wrap(buffer, position + LENGTH_OFFSET, LENGTH_LENGTH)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .getInt();
            int totalBatchSize = LOG_OVERHEAD + batchLength;

            if (totalBatchSize <= 0) {
                // Invalid batch length, stop parsing.
                break;
            }

            if (position + totalBatchSize > bufferLength) {
                // This batch extends beyond the buffer boundary.
                break;
            }

            position += totalBatchSize;
            lastCompleteBatchEnd = position;
        }

        return lastCompleteBatchEnd;
    }

    /**
     * Handles the case where a single batch is larger than the chunk size. Reads the batch header
     * to determine the full batch size, then reads the entire batch into memory.
     */
    private MemoryLogRecords readOversizedBatch(byte[] partialBuffer, int partialLength)
            throws IOException {
        if (partialLength < LOG_OVERHEAD) {
            // Not enough data even for a batch header, mark as exhausted.
            currentPosition = endPosition;
            return MemoryLogRecords.EMPTY;
        }

        // Read the batch length from the partial buffer.
        // Fluss uses little-endian byte order for the batch header.
        int batchLength =
                ByteBuffer.wrap(partialBuffer, LENGTH_OFFSET, LENGTH_LENGTH)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt();
        int totalBatchSize = LOG_OVERHEAD + batchLength;

        if (totalBatchSize <= 0 || currentPosition + totalBatchSize > endPosition) {
            LOG.warn(
                    "Encountered invalid batch size {} at position {}, skipping remaining segment.",
                    totalBatchSize,
                    currentPosition);
            currentPosition = endPosition;
            return MemoryLogRecords.EMPTY;
        }

        // Allocate a buffer for the full batch and copy existing data.
        byte[] fullBuffer = new byte[totalBatchSize];
        System.arraycopy(partialBuffer, 0, fullBuffer, 0, partialLength);

        // Read the remaining bytes of the oversized batch.
        int remainingToRead = totalBatchSize - partialLength;
        int totalRead = readFully(fullBuffer, partialLength, remainingToRead);

        if (totalRead < remainingToRead) {
            LOG.warn(
                    "Unexpected EOF reading oversized batch at position {}, "
                            + "expected {} more bytes but got {}.",
                    currentPosition,
                    remainingToRead,
                    totalRead);
            currentPosition = endPosition;
            return MemoryLogRecords.EMPTY;
        }

        currentPosition += totalBatchSize;
        return MemoryLogRecords.pointToBytes(fullBuffer);
    }

    /** Reads exactly {@code length} bytes into the buffer, retrying on partial reads. */
    private int readFully(byte[] buffer, int offset, int length) throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int read = inputStream.read(buffer, offset + totalRead, length - totalRead);
            if (read < 0) {
                break;
            }
            totalRead += read;
        }
        return totalRead;
    }

    /** Returns true if the entire segment has been read. */
    boolean isExhausted() {
        return currentPosition >= endPosition;
    }

    @Override
    public void close() {
        try {
            inputStream.close();
        } catch (IOException e) {
            LOG.warn("Failed to close remote segment input stream.", e);
        }
    }
}
