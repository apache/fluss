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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.record.bytesview.FlussFileRegion;
import org.apache.fluss.rpc.netty.ssl.SslContextFactory;
import org.apache.fluss.rpc.netty.ssl.TestSslUtils;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.util.SelfSignedCertificate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TlsFileRegionHandler}. */
class TlsFileRegionHandlerTest {

    // larger than one TLS record (16 KiB), so the copy is encrypted in several records.
    private static final byte[] RECORDS = repeat("fluss-log-records-", 2048);

    @TempDir private Path tempDir;

    private SslHandler serverSsl;
    private SslHandler clientSsl;
    private Path logFile;

    @BeforeEach
    void setup() throws Exception {
        SelfSignedCertificate cert = TestSslUtils.generateCertificate("localhost");
        Path keyStore = TestSslUtils.createKeyStore(tempDir, "keystore.jks", cert);
        Path trustStore = TestSslUtils.createTrustStore(tempDir, "truststore.jks", cert);
        Configuration serverConf = new Configuration();
        TestSslUtils.setServerSslConfig(serverConf, keyStore, null);
        Configuration clientConf = new Configuration();
        TestSslUtils.setClientSslConfig(clientConf, trustStore, null);

        serverSsl =
                SslContextFactory.createServerSslHandler(
                        SslContextFactory.createServerSslContext(serverConf).get(),
                        ByteBufAllocator.DEFAULT,
                        false);
        clientSsl =
                SslContextFactory.createClientSslHandler(
                        SslContextFactory.createClientSslContext(clientConf).get(),
                        ByteBufAllocator.DEFAULT,
                        "localhost",
                        9123,
                        "");
        logFile = Files.write(tempDir.resolve("log"), RECORDS);
    }

    @Test
    void testFileRegionIsEncryptedOnTlsChannel() throws Exception {
        EmbeddedChannel server = new EmbeddedChannel(serverSsl, new TlsFileRegionHandler());
        EmbeddedChannel client = new EmbeddedChannel(clientSsl);
        handshake(server, client);

        try (FileChannel file = FileChannel.open(logFile, StandardOpenOption.READ)) {
            FlussFileRegion region = new FlussFileRegion(file, 0, RECORDS.length);
            ChannelFuture write = server.writeAndFlush(region);

            assertThat(write.isSuccess()).isTrue();
            assertThat(region.refCnt()).isZero();
            transfer(server, client);
            assertThat(readInbound(client)).isEqualTo(RECORDS);
        } finally {
            server.finishAndReleaseAll();
            client.finishAndReleaseAll();
        }
    }

    @Test
    void testFileRegionFailsOnTlsChannelWithoutHandler() throws Exception {
        // documents why the handler exists: SslHandler accepts only ByteBufs.
        EmbeddedChannel server = new EmbeddedChannel(serverSsl);
        EmbeddedChannel client = new EmbeddedChannel(clientSsl);
        handshake(server, client);

        try (FileChannel file = FileChannel.open(logFile, StandardOpenOption.READ)) {
            ChannelFuture write =
                    server.writeAndFlush(new FlussFileRegion(file, 0, RECORDS.length));

            assertThat(write.isSuccess()).isFalse();
        } finally {
            server.finishAndReleaseAll();
            client.finishAndReleaseAll();
        }
    }

    @Test
    void testFileRegionIsCopiedInChunks() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new TlsFileRegionHandler());
        try (FileChannel file = FileChannel.open(logFile, StandardOpenOption.READ)) {
            FlussFileRegion region = new FlussFileRegion(file, 0, RECORDS.length);
            channel.writeAndFlush(region);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int chunks = 0;
            ByteBuf chunk;
            while ((chunk = channel.readOutbound()) != null) {
                assertThat(chunk.readableBytes())
                        .isLessThanOrEqualTo(TlsFileRegionHandler.CHUNK_SIZE);
                byte[] bytes = new byte[chunk.readableBytes()];
                chunk.readBytes(bytes);
                out.write(bytes, 0, bytes.length);
                chunk.release();
                chunks++;
            }
            assertThat(chunks).isGreaterThan(1);
            assertThat(out.toByteArray()).isEqualTo(RECORDS);
            assertThat(region.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void testFailedCopyClosesChannel() throws Exception {
        // the region claims more bytes than the file holds, as after a truncation mid-fetch:
        // the first chunk is already sent when the copy fails.
        EmbeddedChannel channel = new EmbeddedChannel(new TlsFileRegionHandler());
        try (FileChannel file = FileChannel.open(logFile, StandardOpenOption.READ)) {
            FlussFileRegion region = new FlussFileRegion(file, 0, RECORDS.length + 100);
            ChannelFuture write = channel.writeAndFlush(region);

            assertThat(write.isSuccess()).isFalse();
            assertThat(region.refCnt()).isZero();
            assertThat(channel.isOpen()).isFalse();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void testOtherMessagesPassThrough() {
        ChannelHandler handler = new TlsFileRegionHandler();
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ByteBuf buf = channel.alloc().buffer().writeBytes(RECORDS);

        channel.writeOutbound(buf);

        assertThat((Object) channel.readOutbound()).isSameAs(buf);
        buf.release();
        channel.finishAndReleaseAll();
    }

    private void handshake(EmbeddedChannel server, EmbeddedChannel client) {
        for (int i = 0;
                i < 20
                        && !(serverSsl.handshakeFuture().isDone()
                                && clientSsl.handshakeFuture().isDone());
                i++) {
            transfer(client, server);
            transfer(server, client);
        }
        assertThat(serverSsl.handshakeFuture().isSuccess()).isTrue();
        assertThat(clientSsl.handshakeFuture().isSuccess()).isTrue();
    }

    private static void transfer(EmbeddedChannel from, EmbeddedChannel to) {
        Object msg;
        while ((msg = from.readOutbound()) != null) {
            to.writeInbound(msg);
        }
    }

    private static byte[] readInbound(EmbeddedChannel channel) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuf buf;
        while ((buf = channel.readInbound()) != null) {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            out.write(bytes, 0, bytes.length);
            buf.release();
        }
        return out.toByteArray();
    }

    private static byte[] repeat(String s, int times) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times; i++) {
            sb.append(s);
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
