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

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.record.send.ByteBufWritableOutput;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * A {@link LogRecords} that represented as a view of bytes. The bytes may be a sequence of bytes
 * references (e.g., {@link MultiBytesView}) that represent projection on a {@link FileLogRecords}.
 */
public class BytesViewLogRecords implements LogRecords {

    private final BytesView bytesView;

    public BytesViewLogRecords(BytesView bytesView) {
        this.bytesView = bytesView;
    }

    public BytesView getBytesView() {
        return bytesView;
    }

    @Override
    public int sizeInBytes() {
        return bytesView.getBytesLength();
    }

    /** This is mainly used for testing. */
    @Override
    public Iterable<LogRecordBatch> batches() {
        ByteBuf buf = Unpooled.buffer(sizeInBytes());
        ByteBufWritableOutput output = new ByteBufWritableOutput(buf);
        output.writeBytes(bytesView);
        checkState(buf.hasArray());
        checkState(buf.arrayOffset() == 0 && buf.readerIndex() == 0);
        checkState(buf.readableBytes() == sizeInBytes());
        MemorySegment segment = MemorySegment.wrap(buf.array());
        MemoryLogRecords logRecords = MemoryLogRecords.pointToMemory(segment, 0, sizeInBytes());
        return logRecords.batches();
    }
}
