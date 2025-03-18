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

package com.alibaba.fluss.fs.gs;

import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import com.alibaba.fluss.shaded.netty4.io.netty.util.AsciiString;
import com.alibaba.fluss.utils.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static com.alibaba.fluss.shaded.guava32.com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static com.alibaba.fluss.shaded.guava32.com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;

/** Netty Handler for facilitating the Google auth token generation. */
public class AuthServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            try {
                URI url = URI.create(req.uri());
                if (req.method().equals(HttpMethod.POST)) {
                    postRequest(ctx, url, req);
                } else {
                    getRequest(ctx, url, req);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void postRequest(ChannelHandlerContext ctx, URI url, HttpRequest req)
            throws IOException {
        if (url.getPath().endsWith("/token")) {
            jsonResponse(ctx, req, "create-token.json");
        } else {
            response(ctx, req, new byte[] {}, NOT_FOUND, APPLICATION_JSON);
        }
    }

    private void getRequest(ChannelHandlerContext ctx, URI url, HttpRequest req)
            throws IOException {
        if (url.getPath().endsWith("/token")) {
            jsonResponse(ctx, req, "create-token.json");
        } else {
            response(ctx, req, new byte[] {}, NOT_FOUND, APPLICATION_JSON);
        }
    }

    private void jsonResponse(ChannelHandlerContext ctx, HttpRequest req, String path)
            throws IOException {
        jsonResponse(ctx, req, path, OK);
    }

    private void jsonResponse(
            ChannelHandlerContext ctx,
            HttpRequest req,
            String path,
            HttpResponseStatus responseStatus)
            throws IOException {
        response(ctx, req, readFromResources(path), responseStatus, APPLICATION_JSON);
    }

    private static void response(
            ChannelHandlerContext ctx,
            HttpRequest req,
            byte[] bytes,
            HttpResponseStatus status,
            AsciiString contentType) {
        FullHttpResponse response =
                new DefaultFullHttpResponse(
                        req.protocolVersion(), status, Unpooled.wrappedBuffer(bytes));
        response.headers()
                .set(CONTENT_TYPE, contentType)
                .set("Location", "http://localhost:8080/resumbable-upload")
                .setInt(CONTENT_LENGTH, response.content().readableBytes());

        response.headers().remove(CONTENT_ENCODING);
        response.headers().set(CONNECTION, CLOSE);
        ctx.write(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

    private byte[] readFromResources(String path) throws IOException {
        InputStream inputStream =
                AuthServerHandler.class.getClassLoader().getResourceAsStream(path);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyBytes(inputStream, out, true);
        return out.toByteArray();
    }
}
