/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.metrics.datadog;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Password;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonEncoding;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/** Sends {@link DatadogSeries} to the Datadog v2 series API. */
class DatadogHttpClient {

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 30_000;
    private static final int MAX_ERROR_BODY_LENGTH = 1024;

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private final URL seriesUrl;
    private final URL validateUrl;
    private final Password apiKey;
    @Nullable private final Proxy proxy;
    private final long intervalSeconds;

    DatadogHttpClient(
            URL seriesUrl,
            URL validateUrl,
            Password apiKey,
            @Nullable Proxy proxy,
            long intervalSeconds) {
        this.seriesUrl = seriesUrl;
        this.validateUrl = validateUrl;
        this.apiKey = apiKey;
        this.proxy = proxy;
        this.intervalSeconds = intervalSeconds;
    }

    @VisibleForTesting
    URL seriesUrl() {
        return seriesUrl;
    }

    @VisibleForTesting
    @Nullable
    Proxy proxy() {
        return proxy;
    }

    /**
     * Returns whether Datadog accepts the API key.
     *
     * <p>The series API answers metrics sent with an unknown key as if it had accepted them, so a
     * wrong key can only be detected through this check.
     */
    boolean isApiKeyValid() throws IOException {
        HttpURLConnection connection = open(validateUrl);
        try {
            connection.setRequestMethod("GET");
            int status = connection.getResponseCode();
            if (status == 403) {
                return false;
            }
            if (status < 200 || status >= 300) {
                throw new IOException(
                        "Datadog answered the API key check with HTTP "
                                + status
                                + ": "
                                + readErrorBody(connection));
            }
            try (InputStream in = connection.getInputStream()) {
                drain(in);
            }
            return true;
        } finally {
            connection.disconnect();
        }
    }

    /** Sends the series in one request, throwing if Datadog does not accept them. */
    void send(List<DatadogSeries> series, long timestampSeconds) throws IOException {
        byte[] body = gzip(toJson(series, timestampSeconds));
        HttpURLConnection connection = open(seriesUrl);
        try {
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Content-Encoding", "gzip");
            connection.setFixedLengthStreamingMode(body.length);
            try (OutputStream out = connection.getOutputStream()) {
                out.write(body);
            }
            int status = connection.getResponseCode();
            if (status < 200 || status >= 300) {
                throw new IOException(
                        "Datadog rejected "
                                + series.size()
                                + " series with HTTP "
                                + status
                                + ": "
                                + readErrorBody(connection));
            }
            // Draining the response lets the connection be reused for the next request.
            try (InputStream in = connection.getInputStream()) {
                drain(in);
            }
        } finally {
            connection.disconnect();
        }
    }

    private HttpURLConnection open(URL url) throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection)
                        (proxy == null ? url.openConnection() : url.openConnection(proxy));
        connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
        connection.setReadTimeout(READ_TIMEOUT_MS);
        connection.setRequestProperty("DD-API-KEY", apiKey.value());
        return connection;
    }

    byte[] toJson(List<DatadogSeries> series, long timestampSeconds) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (JsonGenerator json = JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8)) {
            json.writeStartObject();
            json.writeArrayFieldStart("series");
            for (DatadogSeries s : series) {
                json.writeStartObject();
                json.writeStringField("metric", s.metric());
                json.writeNumberField("type", s.type().code());
                if (s.type() == DatadogSeries.Type.COUNT) {
                    json.writeNumberField("interval", intervalSeconds);
                }
                json.writeArrayFieldStart("points");
                json.writeStartObject();
                json.writeNumberField("timestamp", timestampSeconds);
                json.writeNumberField("value", s.value());
                json.writeEndObject();
                json.writeEndArray();
                json.writeArrayFieldStart("tags");
                for (String tag : s.tags()) {
                    json.writeString(tag);
                }
                json.writeEndArray();
                if (s.host() != null) {
                    json.writeArrayFieldStart("resources");
                    json.writeStartObject();
                    json.writeStringField("name", s.host());
                    json.writeStringField("type", "host");
                    json.writeEndObject();
                    json.writeEndArray();
                }
                json.writeEndObject();
            }
            json.writeEndArray();
            json.writeEndObject();
        }
        return out.toByteArray();
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(data);
        }
        return out.toByteArray();
    }

    private static String readErrorBody(HttpURLConnection connection) {
        try (InputStream in = connection.getErrorStream()) {
            if (in == null) {
                return "";
            }
            byte[] buffer = new byte[MAX_ERROR_BODY_LENGTH];
            int length = 0;
            int read;
            while (length < buffer.length
                    && (read = in.read(buffer, length, buffer.length - length)) != -1) {
                length += read;
            }
            return new String(buffer, 0, length, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "";
        }
    }

    private static void drain(InputStream in) throws IOException {
        byte[] buffer = new byte[1024];
        while (in.read(buffer) != -1) {
            // discard
        }
    }
}
