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

package org.apache.fluss.fs.gs;

import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.fluss.shaded.netty4.io.netty.util.AsciiString;
import org.apache.fluss.utils.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.apache.fluss.shaded.guava32.com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static org.apache.fluss.shaded.guava32.com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * Netty handler for the mock Google token endpoint. Token requests are answered with a fixed bearer
 * token, and the requested scope is recorded so tests can assert the read-only scope.
 */
public class AuthServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    private final List<String> requestedScopes;
    private final List<String> requestedIssuers;

    AuthServerHandler(List<String> requestedScopes, List<String> requestedIssuers) {
        this.requestedScopes = requestedScopes;
        this.requestedIssuers = requestedIssuers;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest req = (FullHttpRequest) msg;

            try {
                URI url = URI.create(req.uri());
                if (url.getPath().endsWith("/token")) {
                    recordTokenExchange(req);
                    jsonResponse(ctx, req, "create-token.json");
                } else {
                    response(ctx, req, new byte[] {}, NOT_FOUND, APPLICATION_JSON);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Service-account refresh posts a JWT whose payload {@code scope} is the requested scope.
     * Compute Engine refresh sends that scope as the {@code scopes} query parameter.
     */
    private void recordTokenExchange(FullHttpRequest req) {
        String scope = "";
        String issuer = "";
        if (req.method().equals(HttpMethod.GET)) {
            scope = firstParameter(new QueryStringDecoder(req.uri()), "scopes");
        } else if (req.method().equals(HttpMethod.POST)) {
            String body = req.content().toString(StandardCharsets.UTF_8);
            // The body is a form, not a path. hasPath=false keeps the decoder from dropping it.
            QueryStringDecoder form = new QueryStringDecoder(body, false);
            String assertion = firstParameter(form, "assertion");
            if (!assertion.isEmpty()) {
                String payload = jwtPayload(assertion);
                scope = jsonStringField(payload, "scope");
                issuer = jsonStringField(payload, "iss");
            }
        }
        requestedScopes.add(scope);
        requestedIssuers.add(issuer);
    }

    private static String firstParameter(QueryStringDecoder decoder, String name) {
        List<String> values = decoder.parameters().get(name);
        if (values == null || values.isEmpty() || values.get(0) == null) {
            return "";
        }
        return values.get(0);
    }

    private static String jwtPayload(String jwt) {
        String[] parts = jwt.split("\\.");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Token request assertion is not a JWT");
        }
        return new String(decodeBase64Url(parts[1]), StandardCharsets.UTF_8);
    }

    private static byte[] decodeBase64Url(String value) {
        String padded = value;
        int remainder = value.length() % 4;
        if (remainder == 2) {
            padded = value + "==";
        } else if (remainder == 3) {
            padded = value + "=";
        }
        return Base64.getUrlDecoder().decode(padded);
    }

    /** Reads a top-level JSON string field. The JWT payload produced here has no nested objects. */
    private static String jsonStringField(String json, String field) {
        String key = "\"" + field + "\"";
        int keyIndex = json.indexOf(key);
        if (keyIndex < 0) {
            return "";
        }
        int colon = json.indexOf(':', keyIndex + key.length());
        int start = json.indexOf('"', colon + 1);
        int end = start < 0 ? -1 : json.indexOf('"', start + 1);
        if (colon < 0 || start < 0 || end < 0) {
            return "";
        }
        return json.substring(start + 1, end);
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
                .set("Metadata-Flavor", "Google")
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
