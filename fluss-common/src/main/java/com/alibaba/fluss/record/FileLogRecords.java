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

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.record.FileLogInputStream.FileChannelLogRecordBatch;
import com.alibaba.fluss.utils.AbstractIterator;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An entity used to describe multiple {@link LogRecordBatch}s in File.
 *
 * @since 0.1
 */
@PublicEvolving
public class FileLogRecords implements LogRecords, Closeable {

    private final boolean isSlice;
    private final int start;
    private final int end;

    // mutable state
    private final AtomicInteger size;
    private volatile File file;
    private final FileChannel channel;

    FileLogRecords(File file, FileChannel channel, int start, int end, boolean isSlice)
            throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;

        size = new AtomicInteger();

        if (isSlice) {
            // don't check the file size if this is just a slice view
            size.set(end - start);
        } else {
            if (channel.size() > Integer.MAX_VALUE) {
                throw new FlussRuntimeException(
                        "The size of segment "
                                + file
                                + " ("
                                + channel.size()
                                + ") is larger than the maximum allowed segment size of "
                                + Integer.MAX_VALUE);
            }

            int limit = Math.min((int) channel.size(), end);
            size.set(limit - start);

            // if this is not a slice, update the file pointer to the end of the file
            // set the file position to the last byte in the file
            channel.position(limit);
        }
    }

    /**
     * Get the underlying file.
     *
     * @return The file
     */
    public File file() {
        return file;
    }

    /**
     * Get the underlying file channel.
     *
     * @return The file channel
     */
    public FileChannel channel() {
        return channel;
    }

    /**
     * Read log batches into the given buffer until there are no bytes remaining in the buffer or
     * the end of the file is reached.
     *
     * @param buffer The buffer to write the batches to
     * @param position Position in the buffer to read from
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)}
     *     for details on the possible exceptions
     */
    public void readInto(ByteBuffer buffer, int position) throws IOException {
        FileUtils.readFully(channel, buffer, position + this.start);
        buffer.flip();
    }

    /**
     * Return a slice of records from this instance, which is a view into this set starting from the
     * given position and with the given size limit.
     *
     * <p>If the size is beyond the end of the file, the end will be based on the size of the file
     * at the time of the read.
     *
     * <p>If this message set is already sliced, the position will be taken relative to that
     * slicing.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    public FileLogRecords slice(int position, int size) throws IOException {
        int availableBytes = availableBytes(position, size);
        int startPosition = this.start + position;
        return new FileLogRecords(
                file, channel, startPosition, startPosition + availableBytes, true);
    }

    private int availableBytes(int position, int size) {
        // Cache current size in case concurrent write changes it
        int currentSizeInBytes = sizeInBytes();

        if (position < 0) {
            throw new IllegalArgumentException(
                    "Invalid position: " + position + " in read from " + this);
        }

        if (position > currentSizeInBytes - start) {
            throw new IllegalArgumentException(
                    "Slice from position " + position + " exceeds end position of " + this);
        }

        if (size < 0) {
            throw new IllegalArgumentException("Invalid size: " + size + " in read from " + this);
        }

        int end = this.start + position + size;
        // Handle integer overflow or if end is beyond the end of the file
        if (end < 0 || end > start + currentSizeInBytes) {
            end = this.start + currentSizeInBytes;
        }
        return end - (this.start + position);
    }

    public int append(MemoryLogRecords records) throws IOException {
        if (records.sizeInBytes() > Integer.MAX_VALUE - size.get()) {
            throw new IllegalArgumentException(
                    "Append of size "
                            + records.sizeInBytes()
                            + " bytes is too large for segment with current file position at "
                            + size.get());
        }

        int written = records.writeFullyTo(channel);
        size.getAndAdd(written);
        return written;
    }

    /** Commit all written data to the physical disk. */
    public void flush() throws IOException {
        channel.force(true);
    }

    /** Close this record set. */
    public void close() throws IOException {
        flush();
        trim();
        channel.close();
    }

    /**
     * Close file handlers used by the FileChannel but don't write to disk. This is used when the
     * disk may have failed.
     */
    public void closeHandlers() throws IOException {
        channel.close();
    }

    /**
     * Delete this message set from the filesystem.
     *
     * @throws IOException if deletion fails due to an I/O error
     * @return {@code true} if the file was deleted by this method; {@code false} if the file could
     *     not be deleted because it did not exist
     */
    public boolean deleteIfExists() throws IOException {
        IOUtils.closeQuietly(channel, "FileChannel");
        return Files.deleteIfExists(file.toPath());
    }

    /** Trim file when close or roll to next file. */
    public void trim() throws IOException {
        truncateTo(sizeInBytes());
    }

    /**
     * Update the parent directory (to be used with caution since this does not reopen the file
     * channel).
     *
     * @param parentDir The new parent directory
     */
    public void updateParentDir(File parentDir) {
        this.file = new File(parentDir, file.getName());
    }

    /**
     * Truncate this file message set to the given size in bytes. Note that this API does no
     * checking that the given size falls on a valid message boundary. In some versions of the JDK
     * truncating to the same size as the file message set will cause an update of the files mtime,
     * so truncate is only performed if the targetSize is smaller than the size of the underlying
     * FileChannel. It is expected that no other threads will do writes to the log when this
     * function is called.
     *
     * @param targetSize The size to truncate to. Must be between 0 and sizeInBytes.
     * @return The number of bytes truncated off
     */
    public int truncateTo(int targetSize) throws IOException {
        int originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0) {
            throw new FlussRuntimeException(
                    "Attempt to truncate log segment "
                            + file
                            + " to "
                            + targetSize
                            + " bytes failed, "
                            + " size of this log segment is "
                            + originalSize
                            + " bytes.");
        }
        if (targetSize < (int) channel.size()) {
            channel.truncate(targetSize);
            size.set(targetSize);
        }
        return originalSize - targetSize;
    }

    public int sizeInBytes() {
        return size.get();
    }

    @Override
    public Iterable<LogRecordBatch> batches() {
        Iterable<FileChannelLogRecordBatch> it = batchesFrom(start);
        return () -> {
            Iterator<FileChannelLogRecordBatch> iterator = it.iterator();
            return new Iterator<LogRecordBatch>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public LogRecordBatch next() {
                    return iterator.next();
                }
            };
        };
    }

    /**
     * Get an iterator over the record batches in the file, starting at a specific position. This is
     * similar to {@link #batches()} except that callers specify a particular position to start
     * reading the batches from. This method must be used with caution: the start position passed in
     * must be a known start of a batch.
     *
     * @return An iterator over batches starting from {@code start}
     */
    private Iterable<FileChannelLogRecordBatch> batchesFrom(final int start) {
        return () -> {
            try {
                return batchIterator(start);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public FileChannelChunk toChunk() {
        return new FileChannelChunk(channel, start, sizeInBytes());
    }

    private AbstractIterator<FileChannelLogRecordBatch> batchIterator(int start)
            throws IOException {
        final int end;
        if (isSlice) {
            end = this.end;
        } else {
            end = this.sizeInBytes();
        }

        FileLogInputStream inputStream = new FileLogInputStream(this, start, end);
        return new LogRecordBatchIterator<>(inputStream);
    }

    /**
     * Rename the file that backs this message set.
     *
     * @throws IOException if rename fails.
     */
    public void renameTo(File f) throws IOException {
        try {
            FileUtils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
        } finally {
            this.file = f;
        }
    }

    /**
     * Search forward for the file position of the last offset that is greater than or equal to the
     * target offset and return its physical position and the size of the message (including log
     * overhead) at the returned offset. If no such offsets are found, return null.
     *
     * @param targetOffset The offset to search for.
     * @param startingPosition The starting position in the file to begin searching from.
     */
    public LogOffsetPosition searchForOffsetWithSize(long targetOffset, int startingPosition) {
        for (FileChannelLogRecordBatch batch : batchesFrom(startingPosition)) {
            long offset = batch.lastLogOffset();
            if (offset >= targetOffset) {
                return new LogOffsetPosition(offset, batch.position(), batch.sizeInBytes());
            }
        }
        return null;
    }

    /**
     * Search forward for the first batch that meets the following requirements:
     *
     * <pre>
     *  - recordBatch's commit timestamp is greater than or equals to the targetTimestamp.
     *  - recordBatch's position in the log file is greater than or equals to the startingPosition.
     *  - recordBatch's offset is greater than or equals to the startingOffset.
     * </pre>
     *
     * @param targetTimestamp The timestamp to search for.
     * @param startingPosition The starting position to search.
     * @param startingOffset The starting offset to search.
     * @return The timestamp and offset of the message found. Null if no message is found.
     */
    public @Nullable TimestampAndOffset searchForTimestamp(
            long targetTimestamp, int startingPosition, long startingOffset) {
        for (LogRecordBatch batch : batchesFrom(startingPosition)) {
            long commitTimestamp = batch.commitTimestamp();
            long baseLogOffset = batch.baseLogOffset();
            if (commitTimestamp >= targetTimestamp && baseLogOffset >= startingOffset) {
                return new TimestampAndOffset(commitTimestamp, baseLogOffset);
            }
        }
        return null;
    }

    /** Return the largest timestamp after a given position in this file. */
    public TimestampAndOffset largestTimestampAfter(int startPosition) {
        long maxTimestamp = -1L;
        long startOffsetOfMaxTimestamp = -1L;

        for (LogRecordBatch batch : batchesFrom(startPosition)) {
            long commitTimestamp = batch.commitTimestamp();
            if (commitTimestamp > maxTimestamp) {
                maxTimestamp = commitTimestamp;
                startOffsetOfMaxTimestamp = batch.baseLogOffset();
            }
        }

        return new TimestampAndOffset(maxTimestamp, startOffsetOfMaxTimestamp);
    }

    @Override
    public String toString() {
        return "FileRecords(size="
                + sizeInBytes()
                + ", file="
                + file
                + ", start="
                + start
                + ", end="
                + end
                + ")";
    }

    public static FileLogRecords open(
            File file,
            boolean mutable,
            boolean fileAlreadyExists,
            int initFileSize,
            boolean preallocate)
            throws IOException {
        FileChannel channel =
                openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate);
        int end = (!fileAlreadyExists && preallocate) ? 0 : Integer.MAX_VALUE;
        return new FileLogRecords(file, channel, 0, end, false);
    }

    public static FileLogRecords open(
            File file, boolean fileAlreadyExists, int initFileSize, boolean preallocate)
            throws IOException {
        return open(file, true, fileAlreadyExists, initFileSize, preallocate);
    }

    public static FileLogRecords open(File file, boolean mutable) throws IOException {
        return open(file, mutable, false, 0, false);
    }

    public static FileLogRecords open(File file) throws IOException {
        return open(file, true);
    }

    /**
     * Open a channel for the given file For windows NTFS and some old LINUX file system, set
     * preallocate to true and initFileSize with one value (for example 512 * 1025 *1024 ) can
     * improve the fluss produce performance.
     *
     * @param file File path
     * @param mutable mutable
     * @param fileAlreadyExists File already exists or not
     * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
     * @param preallocate Pre-allocate file or not, gotten from configuration.
     */
    private static FileChannel openChannel(
            File file,
            boolean mutable,
            boolean fileAlreadyExists,
            int initFileSize,
            boolean preallocate)
            throws IOException {
        if (mutable) {
            if (fileAlreadyExists || !preallocate) {
                return FileChannel.open(
                        file.toPath(),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
            } else {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(initFileSize);
                return randomAccessFile.getChannel();
            }
        } else {
            return FileChannel.open(file.toPath());
        }
    }

    /**
     * A position in a log file.
     *
     * @since 0.1
     */
    @PublicEvolving
    public static class LogOffsetPosition {
        public final long offset;
        public final int position;
        public final int size;

        public LogOffsetPosition(long offset, int position, int size) {
            this.offset = offset;
            this.position = position;
            this.size = size;
        }

        public long getOffset() {
            return offset;
        }

        public int getPosition() {
            return position;
        }

        public int getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LogOffsetPosition that = (LogOffsetPosition) o;

            return offset == that.offset && position == that.position && size == that.size;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(offset);
            result = 31 * result + position;
            result = 31 * result + size;
            return result;
        }

        @Override
        public String toString() {
            return "LogOffsetPosition("
                    + "offset="
                    + offset
                    + ", position="
                    + position
                    + ", size="
                    + size
                    + ')';
        }
    }
}
