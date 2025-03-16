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

package com.alibaba.fluss.record.bytesview;

import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;

/** A {@link BytesView} that backends on a Netty {@link ByteBuf}. */
public class ByteBufBytesView implements BytesView {

    private final ByteBuf buf;
    private final int length;

    public ByteBufBytesView(ByteBuf buf) {
        this.buf = buf;
        this.length = buf.readableBytes();
    }

    public ByteBufBytesView(byte[] bytes) {
        this(Unpooled.wrappedBuffer(bytes));
    }

    @Override
    public ByteBuf getByteBuf() {
        return buf;
    }

    @Override
    public int getBytesLength() {
        return length;
    }

    @Override
    public int getZeroCopyLength() {
        return 0;
    }
}
