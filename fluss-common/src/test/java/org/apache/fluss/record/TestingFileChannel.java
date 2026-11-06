/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

/** A file channel wrapper that records reads and supports short reads and injected failures. */
final class TestingFileChannel extends FileChannel {
    private final FileChannel delegate;
    private final List<ReadRange> reads = new ArrayList<>();
    private int maxBytesPerRead = Integer.MAX_VALUE;
    private long failurePosition = Long.MAX_VALUE;

    TestingFileChannel(FileChannel delegate) {
        this.delegate = delegate;
    }

    void setMaxBytesPerRead(int maxBytesPerRead) {
        checkArgument(maxBytesPerRead > 0, "Read size must be positive.");
        this.maxBytesPerRead = maxBytesPerRead;
    }

    void failReadsAt(long position) {
        this.failurePosition = position;
    }

    void clearReadFailure() {
        failurePosition = Long.MAX_VALUE;
    }

    List<ReadRange> getReads() {
        return Collections.unmodifiableList(reads);
    }

    long getBytesRead() {
        return reads.stream().mapToLong(read -> read.end - read.start).sum();
    }

    void resetReadTracking() {
        reads.clear();
    }

    /** Asserts that reads stay within the union of ranges relative to the given base offset. */
    void assertReadsWithin(long baseOffset, List<long[]> ranges) {
        assertThat(reads).isNotEmpty();
        List<long[]> sortedRanges = new ArrayList<>(ranges);
        sortedRanges.sort(Comparator.comparingLong(range -> range[0]));
        List<long[]> mergedRanges = new ArrayList<>();
        long[] current = null;
        for (long[] range : sortedRanges) {
            if (range[0] == range[1]) {
                continue;
            }
            if (current == null || range[0] > current[1]) {
                current = range.clone();
                mergedRanges.add(current);
            } else {
                current[1] = Math.max(current[1], range[1]);
            }
        }
        for (ReadRange read : reads) {
            boolean allowed =
                    mergedRanges.stream()
                            .anyMatch(
                                    range ->
                                            read.start >= baseOffset + range[0]
                                                    && read.end <= baseOffset + range[1]);
            assertThat(allowed)
                    .as(
                            "read [%s, %s) must be in the allowed ranges at offset %s",
                            read.start, read.end, baseOffset)
                    .isTrue();
        }
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        if (!dst.hasRemaining()) {
            return 0;
        }
        if (position >= failurePosition) {
            throw new IOException("buffer read failed");
        }
        int limit = dst.limit();
        int length =
                (int)
                        Math.min(
                                Math.min(dst.remaining(), maxBytesPerRead),
                                failurePosition - position);
        try {
            dst.limit(dst.position() + length);
            int read = delegate.read(dst, position);
            if (read > 0) {
                reads.add(new ReadRange(position, position + read, dst.isDirect()));
            }
            return read;
        } finally {
            dst.limit(limit);
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        long position = position();
        int read = read(dst, position);
        if (read > 0) {
            position(position + read);
        }
        return read;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        long total = 0;
        for (int i = offset; i < offset + length; i++) {
            int read = read(dsts[i]);
            if (read < 0) {
                return total == 0 ? -1 : total;
            }
            total += read;
            if (dsts[i].hasRemaining()) {
                break;
            }
        }
        return total;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return delegate.write(src);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        return delegate.write(src, position);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return delegate.write(srcs, offset, length);
    }

    @Override
    public long position() throws IOException {
        return delegate.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        delegate.position(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        return delegate.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        delegate.truncate(size);
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        delegate.force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) {
        throw new UnsupportedOperationException("Use read so that bytes are tracked.");
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count)
            throws IOException {
        return delegate.transferFrom(src, position, count);
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) {
        throw new UnsupportedOperationException("Use read so that bytes are tracked.");
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return delegate.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return delegate.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        delegate.close();
    }

    /** The actual byte range returned by a read, with an exclusive end position. */
    static final class ReadRange {
        final long start;
        final long end;
        final boolean direct;

        private ReadRange(long start, long end, boolean direct) {
            this.start = start;
            this.end = end;
            this.direct = direct;
        }
    }
}
