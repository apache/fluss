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

package org.apache.fluss.memory;

import org.apache.fluss.record.bytesview.MemorySegmentBytesView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The base class for all output views that are backed by multiple memory pages. This base class
 * contains all encoding methods to write data to a page and detect page boundary crossing. The
 * concrete subclasses must implement the methods to collect the current page and provide the next
 * memory page once the boundary is crossed.
 *
 * <p>The paging assumes that all memory segments are of the same size.
 */
public abstract class AbstractPagedOutputView implements OutputView, MemorySegmentWritable {

    /** The page size of each memory segments. */
    protected final int pageSize;

    /** The list of memory segments that have been fully written and are immutable now. */
    private List<MemorySegmentBytesView> finishedPages;

    /** The current memory segment to write to. */
    private MemorySegment currentSegment;

    /** the offset in the current segment. */
    private int positionInSegment;

    protected AbstractPagedOutputView(MemorySegment initialSegment, int pageSize) {
        if (initialSegment == null) {
            throw new NullPointerException("Initial Segment may not be null");
        }
        checkArgument(
                initialSegment.size() == pageSize, "Initial segment size must match page size.");
        this.pageSize = pageSize;
        this.currentSegment = initialSegment;
        this.positionInSegment = 0;
        this.finishedPages = new ArrayList<>();
    }

    // --------------------------------------------------------------------------------------------
    //                                  Page Management
    // --------------------------------------------------------------------------------------------

    /**
     * This method must return a segment. If no more segments are available, it must throw an {@link
     * java.io.EOFException}.
     *
     * @return The next memory segment.
     */
    protected abstract MemorySegment nextSegment() throws IOException;

    /**
     * Gets the list of memory segments that are used by the output view and are allocated from
     * {@link MemorySegmentPool}.
     */
    public abstract List<MemorySegment> allocatedPooledSegments();

    /** Gets the list of memory segments that have been written bytes. */
    public List<MemorySegmentBytesView> getWrittenSegments() {
        List<MemorySegmentBytesView> views = new ArrayList<>(finishedPages.size() + 1);
        views.addAll(finishedPages);
        views.add(new MemorySegmentBytesView(currentSegment, 0, positionInSegment));
        return views;
    }

    /**
     * Gets the segment that the view currently writes to.
     *
     * @return The segment the view currently writes to.
     */
    public MemorySegment getCurrentSegment() {
        return this.currentSegment;
    }

    /**
     * Gets the current write position (the position where the next bytes will be written) in the
     * current memory segment.
     *
     * @return The current write offset in the current memory segment.
     */
    public int getCurrentPositionInSegment() {
        return this.positionInSegment;
    }

    /**
     * Gets the page size of the memory segments.
     *
     * @return the page size of the memory segments.
     */
    public int getPageSize() {
        return this.pageSize;
    }

    /**
     * Sets the position in the current memory segment. This method is intended for use in the
     * context of repositioning the output view to a previous position.
     *
     * <p>NOTE: This method should only be used for the first memory segment.
     */
    public void setPosition(int position) throws IllegalStateException {
        if (!finishedPages.isEmpty()) {
            throw new IllegalStateException("Cannot set position for non-first memory segment.");
        }
        if (currentSegment.size() <= position) {
            throw new IllegalStateException(
                    "Position "
                            + position
                            + " is out of bounds for segment of size "
                            + this.currentSegment.size());
        }

        this.positionInSegment = position;
    }

    /**
     * Moves the output view to the next page. This method invokes internally the {@link
     * #nextSegment()} method to give the current memory segment to the concrete subclass'
     * implementation and obtain the next segment to write to. Writing will continue inside the new
     * segment after the header.
     *
     * @throws IOException Thrown, if the current segment could not be processed or a new segment
     *     could not be obtained.
     */
    public void advance() throws IOException {
        MemorySegmentBytesView segmentBytesView =
                new MemorySegmentBytesView(currentSegment, 0, positionInSegment);
        this.currentSegment = nextSegment();
        this.positionInSegment = 0;
        finishedPages.add(segmentBytesView);
    }

    /**
     * Clears the internal state. Any successive write calls will fail until either {@link
     * #advance()}.
     *
     * @see #advance()
     */
    public void clear() {
        this.currentSegment = null;
        this.positionInSegment = 0;
        this.finishedPages = new ArrayList<>();
    }

    // --------------------------------------------------------------------------------------------
    //                               Data Output Specific methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void writeByte(int v) throws IOException {
        if (positionInSegment < pageSize) {
            currentSegment.put(positionInSegment++, (byte) v);
        } else {
            advance();
            writeByte(v);
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int remaining = pageSize - positionInSegment;
        if (remaining >= len) {
            currentSegment.put(positionInSegment, b, off, len);
            positionInSegment += len;
        } else {
            if (remaining == 0) {
                advance();
                remaining = pageSize - positionInSegment;
            }
            while (true) {
                int toPut = Math.min(remaining, len);
                currentSegment.put(positionInSegment, b, off, toPut);
                off += toPut;
                len -= toPut;

                if (len > 0) {
                    positionInSegment = pageSize;
                    advance();
                    remaining = pageSize - positionInSegment;
                } else {
                    positionInSegment += toPut;
                    break;
                }
            }
        }
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        writeByte(v ? 1 : 0);
    }

    @Override
    public void writeShort(int v) throws IOException {
        if (positionInSegment < pageSize - 1) {
            currentSegment.putShort(positionInSegment, (short) v);
            positionInSegment += 2;
        } else if (positionInSegment == pageSize) {
            advance();
            writeShort(v);
        } else {
            // TODO add some test to cover this.
            writeByte(v);
            writeByte(v >> 8);
        }
    }

    @Override
    public void writeInt(int v) throws IOException {
        if (positionInSegment < pageSize - 3) {
            currentSegment.putInt(positionInSegment, v);
            positionInSegment += 4;
        } else if (positionInSegment == pageSize) {
            advance();
            writeInt(v);
        } else {
            writeByte(v);
            writeByte(v >> 8);
            writeByte(v >> 16);
            writeByte(v >> 24);
        }
    }

    public void writeUnsignedInt(long v) throws IOException {
        writeInt((int) (v & 0xffffffffL));
    }

    @Override
    public void writeLong(long v) throws IOException {
        if (positionInSegment < pageSize - 7) {
            currentSegment.putLong(positionInSegment, v);
            positionInSegment += 8;
        } else if (positionInSegment == pageSize) {
            advance();
            writeLong(v);
        } else {
            writeByte((int) v);
            writeByte((int) (v >> 8));
            writeByte((int) (v >> 16));
            writeByte((int) (v >> 24));
            writeByte((int) (v >> 32));
            writeByte((int) (v >> 40));
            writeByte((int) (v >> 48));
            writeByte((int) (v >> 56));
        }
    }

    @Override
    public void writeFloat(float v) throws IOException {
        if (positionInSegment < pageSize - 3) {
            currentSegment.putFloat(positionInSegment, v);
            positionInSegment += 4;
        } else if (positionInSegment == pageSize) {
            advance();
            writeFloat(v);
        } else {
            writeInt(Float.floatToIntBits(v));
        }
    }

    @Override
    public void writeDouble(double v) throws IOException {
        if (positionInSegment < pageSize - 7) {
            currentSegment.putDouble(positionInSegment, v);
            positionInSegment += 8;
        } else if (positionInSegment == pageSize) {
            advance();
            writeDouble(v);
        } else {
            writeLong(Double.doubleToLongBits(v));
        }
    }

    @Override
    public void write(MemorySegment segment, int off, int len) throws IOException {
        int remaining = pageSize - positionInSegment;
        if (remaining >= len) {
            segment.copyTo(off, currentSegment, positionInSegment, len);
            positionInSegment += len;
        } else {
            if (remaining == 0) {
                advance();
                remaining = pageSize - positionInSegment;
            }

            while (true) {
                int toPut = Math.min(remaining, len);
                segment.copyTo(off, currentSegment, positionInSegment, toPut);
                off += toPut;
                len -= toPut;

                if (len > 0) {
                    positionInSegment = pageSize;
                    advance();
                    remaining = pageSize - positionInSegment;
                } else {
                    positionInSegment += toPut;
                    break;
                }
            }
        }
    }
}
