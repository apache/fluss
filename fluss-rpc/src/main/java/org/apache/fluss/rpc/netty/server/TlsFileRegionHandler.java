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
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.FileRegion;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Makes log fetches work on TLS listeners.
 *
 * <p>To answer a log fetch, the server normally hands Netty a {@link FileRegion}: a range of a log
 * file that the OS copies from disk straight to the socket, without the bytes passing through the
 * JVM (zero-copy). But TLS has to encrypt the bytes in memory before they go out. Netty's TLS
 * handler, {@link SslHandler}, therefore only takes bytes already in memory ({@link ByteBuf}) and
 * fails when it is given a {@link FileRegion}.
 *
 * <p>This handler reads each {@link FileRegion} into a {@link ByteBuf} and passes that on to {@link
 * SslHandler} instead. The cost is one extra copy of the fetched records on TLS listeners.
 * Plaintext listeners do not install this handler and keep zero-copy.
 */
final class TlsFileRegionHandler extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (!(msg instanceof FileRegion)) {
            ctx.write(msg, promise);
            return;
        }
        FileRegion region = (FileRegion) msg;
        ByteBuf buf = ctx.alloc().ioBuffer(Math.toIntExact(region.count()));
        try {
            copy(region, buf);
        } catch (Throwable t) {
            buf.release();
            promise.setFailure(t);
            return;
        } finally {
            region.release();
        }
        ctx.write(buf, promise);
    }

    private static void copy(FileRegion region, ByteBuf dst) throws IOException {
        WritableByteChannel sink =
                new WritableByteChannel() {
                    @Override
                    public int write(ByteBuffer src) {
                        int n = src.remaining();
                        dst.writeBytes(src);
                        return n;
                    }

                    @Override
                    public boolean isOpen() {
                        return true;
                    }

                    @Override
                    public void close() {}
                };
        long copied = 0;
        while (copied < region.count()) {
            long n = region.transferTo(sink, copied);
            if (n <= 0) {
                throw new IOException(
                        String.format(
                                "File region ended after %d of %d bytes.", copied, region.count()));
            }
            copied += n;
        }
    }
}
