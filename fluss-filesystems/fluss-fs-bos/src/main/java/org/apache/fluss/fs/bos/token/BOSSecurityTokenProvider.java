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

package org.apache.fluss.fs.bos.token;

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.ACCESS_KEY;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.ENDPOINT;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.REGION;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.SECRET_KEY;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.STS_DURATION_SECONDS;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.STS_ENDPOINT;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.STS_PERMISSIONS;
import static org.apache.fluss.fs.bos.BOSFileSystemPlugin.getLong;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Server side of the BOS delegation-token flow: exchanges the server's long-term AK/SK for a
 * short-lived, read-scoped credential through Baidu BCE STS ({@code POST /v1/sessionToken},
 * bce-auth-v1 signing). BCE STS is not AWS STS, which is why the stock S3 plugin cannot talk to it.
 */
public class BOSSecurityTokenProvider {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter ISO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    private final String stsEndpoint;
    private final String ak;
    private final String sk;
    private final String bucket;
    private final String region;
    private final String endpoint;
    private final List<String> permissions;
    private final long durationSeconds;

    /**
     * Creates a provider from the server side BOS options (prefix already stripped, e.g. {@code
     * access.key}).
     *
     * @param conf the BOS options of the file system
     * @param bucket the bucket the issued credentials are scoped to
     */
    public BOSSecurityTokenProvider(Map<String, String> conf, String bucket) {
        this.ak = conf.get(ACCESS_KEY);
        this.sk = conf.get(SECRET_KEY);
        this.region = conf.get(REGION);
        this.endpoint = conf.get(ENDPOINT);
        checkArgument(
                ak != null && sk != null && region != null,
                "BOS token delegation needs bos.access.key, bos.secret.key and bos.region on the server");
        String e = conf.getOrDefault(STS_ENDPOINT, "https://sts.bj.baidubce.com");
        this.stsEndpoint = e.endsWith("/") ? e.substring(0, e.length() - 1) : e;
        this.permissions = new ArrayList<>();
        for (String p : conf.getOrDefault(STS_PERMISSIONS, "READ,LIST").split(",")) {
            permissions.add(p.trim());
        }
        this.durationSeconds = getLong(conf, STS_DURATION_SECONDS, 43200);
        this.bucket = bucket;
    }

    /**
     * Exchanges the long-term access key for a short-lived credential through BCE STS.
     *
     * @param scheme the scheme the token is issued for
     * @return the obtained token, including its expiration time
     * @throws IOException if the STS request fails
     */
    public ObtainedSecurityToken obtainSecurityToken(String scheme) throws IOException {
        String host = stsEndpoint.replaceFirst("^https?://", "");
        String path = "/v1/sessionToken";
        String query = "durationSeconds=" + durationSeconds;
        String ts = ISO.format(Instant.now());
        StringBuilder perms = new StringBuilder();
        for (String p : permissions) {
            perms.append(perms.length() == 0 ? "" : ",").append('"').append(p).append('"');
        }
        // the bucket itself must be included, otherwise bucket-level calls (e.g. HEAD/list on the
        // bucket)
        // are rejected with 403
        String body =
                "{\"accessControlList\":[{\"service\":\"bce:bos\",\"region\":\""
                        + region
                        + "\",\"effect\":\"Allow\",\"resource\":[\""
                        + bucket
                        + "\",\""
                        + bucket
                        + "/*\"],\"permission\":["
                        + perms
                        + "]}]}";

        String prefix = "bce-auth-v1/" + ak + "/" + ts + "/1800";
        String signingKey = hex(hmac(sk.getBytes(StandardCharsets.UTF_8), prefix));
        TreeMap<String, String> signed = new TreeMap<>();
        signed.put("content-type", "application/json");
        signed.put("host", host);
        signed.put("x-bce-date", ts);
        StringBuilder headers = new StringBuilder();
        for (Map.Entry<String, String> h : signed.entrySet()) {
            headers.append(headers.length() == 0 ? "" : "\n")
                    .append(h.getKey())
                    .append(':')
                    .append(enc(h.getValue()));
        }
        String canonical = "POST\n" + path + "\n" + query + "\n" + headers;
        String signature = hex(hmac(signingKey.getBytes(StandardCharsets.UTF_8), canonical));
        String auth = prefix + "/" + String.join(";", signed.keySet()) + "/" + signature;

        HttpURLConnection c =
                (HttpURLConnection) new URL(stsEndpoint + path + "?" + query).openConnection();
        c.setRequestMethod("POST");
        c.setConnectTimeout(10_000);
        c.setReadTimeout(20_000);
        c.setDoOutput(true);
        c.setRequestProperty("Content-Type", "application/json");
        c.setRequestProperty("x-bce-date", ts);
        c.setRequestProperty("Authorization", auth);
        try (OutputStream os = c.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        int code = c.getResponseCode();
        try (InputStream is = code < 400 ? c.getInputStream() : c.getErrorStream()) {
            String resp = is == null ? "" : new String(readFully(is), StandardCharsets.UTF_8);
            if (code != 200) {
                throw new IOException("BCE STS GetSessionToken failed: http " + code + " " + resp);
            }
            JsonNode j = MAPPER.readTree(resp);
            Instant expiration = Instant.parse(j.path("expiration").asText());
            Map<String, String> additionInfos = new HashMap<>();
            additionInfos.put("endpoint", endpoint);
            additionInfos.put("region", region);
            return new ObtainedSecurityToken(
                    scheme,
                    CredentialsJsonSerde.toJson(
                            new Credentials(
                                    j.path("accessKeyId").asText(),
                                    j.path("secretAccessKey").asText(),
                                    j.path("sessionToken").asText())),
                    expiration.toEpochMilli(),
                    additionInfos);
        }
    }

    // InputStream#readAllBytes is Java 9+, Fluss still builds with the java8 profile
    private static byte[] readFully(InputStream is) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int n;
        while ((n = is.read(chunk)) >= 0) {
            out.write(chunk, 0, n);
        }
        return out.toByteArray();
    }

    private static byte[] hmac(byte[] key, String data) throws IOException {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IOException("HMAC failed", e);
        }
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }

    private static String enc(String s) {
        StringBuilder sb = new StringBuilder();
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            char ch = (char) (b & 0xff);
            if ((ch >= 'A' && ch <= 'Z')
                    || (ch >= 'a' && ch <= 'z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '-'
                    || ch == '_'
                    || ch == '.'
                    || ch == '~') {
                sb.append(ch);
            } else {
                sb.append(String.format("%%%02X", b & 0xff));
            }
        }
        return sb.toString();
    }
}
