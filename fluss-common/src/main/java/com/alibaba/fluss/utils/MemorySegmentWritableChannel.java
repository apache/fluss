/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils;

import com.alibaba.fluss.memory.MemorySegment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/** A {@link WritableByteChannel} that writes to a {@link MemorySegment}. */
public class MemorySegmentWritableChannel implements WritableByteChannel {

    private final MemorySegment output;
    private int position;
    private boolean closed;

    public MemorySegmentWritableChannel(MemorySegment output) {
        this.output = output;
        this.position = 0;
        this.closed = false;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int length = src.remaining();
        output.put(position, src, length);
        position += length;
        return length;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }
}
