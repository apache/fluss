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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.FileRegion;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.stream.ChunkedInput;
import org.apache.fluss.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Lets TLS listeners serve log fetches.
 *
 * <p>The server answers a log fetch with a {@link FileRegion}, a range of a log file. On a
 * plaintext listener the OS sends that range from disk to the socket directly, and the bytes never
 * enter the JVM (zero-copy).
 *
 * <p>TLS cannot do that, because it encrypts bytes in memory, and Netty's {@link SslHandler}
 * accepts only {@link ByteBuf}s. So TLS fails on a disk-based {@link FileRegion} of the log.
 *
 * <p>This handler sits before {@link SslHandler}, so writes pass through it first. It copies each
 * {@link FileRegion} into {@link ByteBuf}s of at most {@link #CHUNK_SIZE} bytes. It copies the next
 * chunk only while the channel is writable, so a connection buffers only a few chunks, not the
 * whole fetch response.
 *
 * <p>Plaintext listeners do not install this handler and keep zero-copy.
 *
 * <p>If a chunk copy fails, the handler closes the connection.
 */
final class TlsFileRegionHandler extends ChunkedWriteHandler {

    static final int CHUNK_SIZE = 32 * 1024;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (!(msg instanceof FileRegion)) {
            super.write(ctx, msg, promise);
            return;
        }
        ChannelPromise closeOnFailure = promise.unvoid();
        closeOnFailure.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        super.write(ctx, new FileRegionInput((FileRegion) msg), closeOnFailure);
    }

    /** Reads a {@link FileRegion} as {@link ByteBuf} chunks; releases the region on close. */
    private static final class FileRegionInput implements ChunkedInput<ByteBuf> {
        private final FileRegion region;
        private long offset;

        FileRegionInput(FileRegion region) {
            this.region = region;
        }

        @Override
        public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
            if (isEndOfInput()) {
                return null;
            }
            ByteBuf chunk = allocator.ioBuffer((int) Math.min(CHUNK_SIZE, region.count() - offset));
            try {
                WritableByteChannel sink = new ChunkSink(chunk);
                while (chunk.isWritable()) {
                    long n = region.transferTo(sink, offset);
                    if (n <= 0) {
                        throw new IOException(
                                String.format(
                                        "File region ended after %d of %d bytes.",
                                        offset, region.count()));
                    }
                    offset += n;
                }
                return chunk;
            } catch (Throwable t) {
                chunk.release();
                throw t;
            }
        }

        @Deprecated
        @Override
        public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
            return readChunk(ctx.alloc());
        }

        @Override
        public boolean isEndOfInput() {
            return offset >= region.count();
        }

        @Override
        public void close() {
            region.release();
        }

        @Override
        public long length() {
            return region.count();
        }

        @Override
        public long progress() {
            return offset;
        }
    }

    /** Accepts only what still fits in the chunk, so a transfer stops once the chunk is full. */
    private static final class ChunkSink implements WritableByteChannel {
        private final ByteBuf chunk;

        ChunkSink(ByteBuf chunk) {
            this.chunk = chunk;
        }

        @Override
        public int write(ByteBuffer src) {
            int n = Math.min(src.remaining(), chunk.writableBytes());
            ByteBuffer part = src.duplicate();
            part.limit(part.position() + n);
            chunk.writeBytes(part);
            src.position(src.position() + n);
            return n;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {}
    }
}
