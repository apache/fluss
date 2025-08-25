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

package com.alibaba.fluss.record.bytesview;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.record.send.WritableOutput;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/** A {@link BytesView} that is consisted of multiple {@link BytesView}s. */
public class MultiBytesView implements BytesView {

    private final BytesView[] views;
    private final int bytesLength;
    private final int zeroCopyLength;

    private MultiBytesView(BytesView[] views) {
        this.views = views;
        int bytesLength = 0;
        int zeroCopyLength = 0;
        for (BytesView view : views) {
            bytesLength += view.getBytesLength();
            zeroCopyLength += view.getZeroCopyLength();
        }
        this.bytesLength = bytesLength;
        this.zeroCopyLength = zeroCopyLength;
    }

    @Override
    public ByteBuf getByteBuf() {
        ByteBuf[] bufs = new ByteBuf[views.length];
        for (int i = 0; i < views.length; i++) {
            bufs[i] = views[i].getByteBuf();
        }
        return Unpooled.wrappedBuffer(bufs.length, bufs);
    }

    @Override
    public int getBytesLength() {
        return bytesLength;
    }

    @Override
    public int getZeroCopyLength() {
        return zeroCopyLength;
    }

    /** Serialize all the bytes into the given {@link WritableOutput}. */
    public void writeTo(WritableOutput output) {
        for (BytesView view : views) {
            output.writeBytes(view);
        }
    }

    // ------------------------------------------------------------------------------------------

    /** Create a new {@link Builder} to build a {@link MultiBytesView}. */
    public static Builder builder() {
        return new Builder();
    }

    /** A builder to build a {@link MultiBytesView}. */
    public static class Builder {

        private final List<BytesView> views = new ArrayList<>();
        private FileRegionBytesView lastFileRegionView = null;

        /** Adds a bytes section from a byte array. */
        public Builder addBytes(byte[] bytes) {
            views.add(new ByteBufBytesView(bytes));
            lastFileRegionView = null;
            return this;
        }

        /** Adds a bytes section from a range of {@link MemorySegment}. */
        public Builder addBytes(MemorySegment memorySegment, int position, int size) {
            views.add(new MemorySegmentBytesView(memorySegment, position, size));
            lastFileRegionView = null;
            return this;
        }

        public Builder addMemorySegmentByteViewList(List<MemorySegmentBytesView> bytesViewList) {
            views.addAll(bytesViewList);
            lastFileRegionView = null;
            return this;
        }

        /** Adds a bytes section from a range of {@link FileChannel}. */
        public Builder addBytes(FileChannel fileChannel, long position, int size) {
            if (lastFileRegionView != null
                    && lastFileRegionView.fileChannel == fileChannel
                    && lastFileRegionView.position + lastFileRegionView.size == position) {
                // merge file region with previous one if they are continuous to improve
                // file read performance.
                lastFileRegionView =
                        new FileRegionBytesView(
                                lastFileRegionView.fileChannel,
                                lastFileRegionView.position,
                                lastFileRegionView.size + size);
                views.set(views.size() - 1, lastFileRegionView);
            } else {
                lastFileRegionView = new FileRegionBytesView(fileChannel, position, size);
                views.add(lastFileRegionView);
            }
            return this;
        }

        /** Builds a {@link MultiBytesView}. */
        public MultiBytesView build() {
            return new MultiBytesView(views.toArray(new BytesView[0]));
        }
    }

    public BytesView[] getBytesViews() {
        return views;
    }

    @VisibleForTesting
    public BytesView getBytesView(int index) {
        return views[index];
    }
}
