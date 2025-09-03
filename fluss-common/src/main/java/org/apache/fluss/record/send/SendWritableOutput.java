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

package org.apache.fluss.record.send;

import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.FileRegionBytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import java.util.ArrayDeque;
import java.util.Queue;

/** A {@link WritableOutput} that builds network {@link Send} with zero-copy optimization. */
public class SendWritableOutput extends ByteBufWritableOutput {

    private final Queue<Send> sends;

    /** The current reader index of the underlying {@link #buf} for building next {@link Send}. */
    private int currentReaderIndex = 0;

    /**
     * @param buf The ByteBuf that has capacity of data size excluding zero-copy.
     */
    public SendWritableOutput(ByteBuf buf) {
        super(buf);
        this.sends = new ArrayDeque<>(1);
    }

    @Override
    public void writeBytes(BytesView val) {
        if (val instanceof FileRegionBytesView) {
            // zero-copy for FileRegion (records on file)
            flushBufferSlice();
            FileRegionBytesView bytesView = (FileRegionBytesView) val;
            sends.add(new FileRegionSend(bytesView.getFileRegion()));
        } else if (val instanceof MemorySegmentBytesView) {
            // zero-copy for MemorySegment (records in memory)
            flushBufferSlice();
            MemorySegmentBytesView bytesView = (MemorySegmentBytesView) val;
            sends.add(new ByteBufSend(bytesView.getByteBuf()));
        } else if (val instanceof MultiBytesView) {
            ((MultiBytesView) val).writeTo(this);
        } else {
            super.writeBytes(val);
        }
    }

    private void flushBufferSlice() {
        if (currentReaderIndex >= buf.readerIndex() && buf.readableBytes() > currentReaderIndex) {
            // there should be remaining data to flush

            // This is important to retain the slice here, because Netty will release the slice
            // after writeAndFlush which will cause reference count of the underlying {@link #buf}
            // decreased to < 0.
            ByteBuf slice =
                    buf.retainedSlice(currentReaderIndex, buf.readableBytes() - currentReaderIndex);
            // next slice should start from the current writer position (= readable bytes)
            currentReaderIndex = buf.readableBytes();
            sends.add(new ByteBufSend(slice));
        }
    }

    public Send buildSend() {
        if (buf.writerIndex() != buf.capacity()) {
            throw new IllegalStateException();
        }
        flushBufferSlice();
        // This is important to release the buf here to avoid leaking.
        // The buf is not deallocated immediately, but will be deallocated once all the retained
        // slices derived from it are released (i.e. after writeAndFlush to Netty).
        buf.release();
        if (sends.size() == 1) {
            return sends.poll();
        } else {
            return new MultiSend(sends);
        }
    }
}
