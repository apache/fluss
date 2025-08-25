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

package org.apache.fluss.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Channel which provides contents from a {@link ByteBuffer}. The {@link ByteBuffer} can be on-heap
 * or off-heap.
 */
public class ByteBufferReadableChannel implements ReadableByteChannel {

    private ByteBuffer buffer;
    private boolean closed;

    public ByteBufferReadableChannel(ByteBuffer buffer) {
        this.buffer = buffer;
        this.closed = false;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (closed) {
            throw new IllegalStateException("The ByteBufferChannel has been closed.");
        }
        if (buffer == null || buffer.remaining() == 0) {
            // clear to allow GC
            buffer = null;
            return -1;
        }

        final int cnt;
        if (buffer.remaining() <= dst.remaining()) {
            cnt = buffer.remaining();
            dst.put(buffer);
            buffer = null;
        } else {
            cnt = dst.remaining();
            ByteBuffer from = buffer.duplicate();
            from.limit(from.position() + dst.remaining());
            dst.put(from);
            buffer.position(from.limit());
        }
        return cnt;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
        // clear to allow GC
        this.buffer = null;
    }
}
