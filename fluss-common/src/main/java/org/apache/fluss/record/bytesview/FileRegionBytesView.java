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

package org.apache.fluss.record.bytesview;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.FileRegion;

import java.io.IOException;
import java.nio.channels.FileChannel;

/** A {@link BytesView} that references a sequence bytes on a local file. */
public class FileRegionBytesView implements BytesView {

    final FileChannel fileChannel;
    final FileRegion fileRegion;
    final long position;
    final int size;

    public FileRegionBytesView(FileChannel fileChannel, long position, int size) {
        this.fileChannel = fileChannel;
        this.fileRegion = new FlussFileRegion(fileChannel, position, size);
        this.position = position;
        this.size = size;
    }

    @Override
    public ByteBuf getByteBuf() {
        // this is expensive and should avoid to be used when it is a FileRegion bytes view
        ByteBuf buf = Unpooled.buffer(size, size);
        try {
            buf.writeBytes(fileChannel, position, size);
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to read from file channel", e);
        }
        return buf;
    }

    @Override
    public int getBytesLength() {
        return size;
    }

    @Override
    public int getZeroCopyLength() {
        return size;
    }

    public FileRegion getFileRegion() {
        return fileRegion;
    }
}
