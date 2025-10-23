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

package org.apache.fluss.testutils;

import org.apache.fluss.record.send.Send;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundInvoker;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelProgressivePromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.FileRegion;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import static org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled.wrappedBuffer;

/** A testing implementation of {@link ChannelOutboundInvoker} that writes to a {@link ByteBuf}. */
public class ByteBufChannel implements ChannelOutboundInvoker {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final WritableByteChannel channel = Channels.newChannel(out);

    public static ByteBuf toByteBuf(Send send) {
        ByteBufChannel channel = new ByteBufChannel();
        try {
            send.writeTo(channel);
            return channel.getByteBuf();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] toByteArray(Send send) {
        ByteBufChannel channel = new ByteBufChannel();
        try {
            send.writeTo(channel);
            return channel.getByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ByteBuf getByteBuf() {
        return wrappedBuffer(out.toByteArray());
    }

    public byte[] getByteArray() {
        return out.toByteArray();
    }

    @Override
    public ChannelFuture bind(SocketAddress socketAddress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture disconnect() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture deregister() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture bind(SocketAddress socketAddress, ChannelPromise channelPromise) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, ChannelPromise channelPromise) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture connect(
            SocketAddress socketAddress,
            SocketAddress socketAddress1,
            ChannelPromise channelPromise) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture disconnect(ChannelPromise channelPromise) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture close(ChannelPromise channelPromise) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture deregister(ChannelPromise channelPromise) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelOutboundInvoker read() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture write(Object o) {
        return write(o, newPromise());
    }

    @Override
    public ChannelFuture write(Object o, ChannelPromise channelPromise) {
        try {
            if (o instanceof FileRegion) {
                FileRegion fr = (FileRegion) o;
                fr.transferTo(channel, fr.transferred());
            } else if (o instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) o;
                ByteBuffer byteBuffer = buf.nioBuffer();
                channel.write(byteBuffer);
            } else {
                throw new IllegalArgumentException("Unsupported type: " + o.getClass());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            ReferenceCountUtil.release(o);
        }
        return null;
    }

    @Override
    public ChannelOutboundInvoker flush() {
        return this;
    }

    @Override
    public ChannelFuture writeAndFlush(Object o, ChannelPromise channelPromise) {
        return write(o, channelPromise);
    }

    @Override
    public ChannelFuture writeAndFlush(Object o) {
        return write(o);
    }

    @Override
    public ChannelPromise newPromise() {
        return null;
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture newSucceededFuture() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelFuture newFailedFuture(Throwable throwable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelPromise voidPromise() {
        throw new UnsupportedOperationException();
    }
}
