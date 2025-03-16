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

import com.alibaba.fluss.memory.AbstractPagedOutputView;
import com.alibaba.fluss.memory.MemorySegment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/** A {@link WritableByteChannel} that writes to a {@link AbstractPagedOutputView}. */
public class PagedMemorySegmentWritableChannel implements WritableByteChannel {

    private final AbstractPagedOutputView output;
    private boolean closed;

    public PagedMemorySegmentWritableChannel(AbstractPagedOutputView output) {
        this.output = output;
        this.closed = false;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int length = src.remaining();
        if (src.isDirect()) {
            // use bulk copy
            MemorySegment segment = MemorySegment.wrapOffHeapMemory(src);
            output.write(segment, src.position(), length);
            src.position(src.position() + length);
        } else if (src.hasArray()) {
            // use bulk copy
            output.write(src.array(), src.position() + src.arrayOffset(), length);
            src.position(src.position() + src.arrayOffset() + length);
        } else {
            // should never go this path.
            for (int i = 0; i < length; i++) {
                output.writeByte(src.get());
            }
        }
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
