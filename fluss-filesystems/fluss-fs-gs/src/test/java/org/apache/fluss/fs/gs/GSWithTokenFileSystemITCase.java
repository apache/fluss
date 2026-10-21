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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.gs.token.GSSecurityTokenReceiver;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

import static org.apache.fluss.fs.FileSystem.WriteMode.NO_OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies a real Cloud Storage bucket accepts the access token minted from a service account key.
 * The client filesystem is created without that key.
 */
@ResourceLock("gs-security-token")
class GSWithTokenFileSystemITCase {

    @AfterEach
    void clearToken() {
        GSSecurityTokenReceiver.reset();
    }

    @Test
    void refreshedTokenCanReadAndWriteWithoutTheServiceAccountKey() throws Exception {
        String keyFile = System.getenv("GS_SA_JSON");
        String bucket = System.getenv("GS_BUCKET");
        assumeTrue(
                keyFile != null && !keyFile.isEmpty() && bucket != null && !bucket.isEmpty(),
                "GS_SA_JSON and GS_BUCKET are required for the real Cloud Storage token test");

        Configuration server = new Configuration();
        server.setString("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
        server.setString("fs.gs.auth.service.account.json.keyfile", keyFile);

        GSFileSystemPlugin plugin = new GSFileSystemPlugin();
        URI bucketUri = URI.create("gs://" + bucket + "/");
        FileSystem serverFs = plugin.create(bucketUri, server);
        ObtainedSecurityToken obtained = serverFs.obtainSecurityToken();
        Credentials minted = CredentialsJsonSerde.fromJson(obtained.getToken());
        assertThat(obtained.getScheme()).isEqualTo("gs");
        assertThat(minted.getSecurityToken()).isNotBlank();
        assertThat(obtained.getValidUntil().get()).isGreaterThan(System.currentTimeMillis());

        Configuration client = new Configuration();
        GSSecurityTokenReceiver.reset();
        installToken("not-a-real-token");
        FileSystem rejected = plugin.create(bucketUri, client);
        FsPath rejectedPath = new FsPath("gs://" + bucket + "/fluss-token-it/" + UUID.randomUUID());
        assertThatThrownBy(() -> write(rejected, rejectedPath, "nope"))
                .isInstanceOf(IOException.class)
                .satisfies(
                        thrown -> {
                            GoogleJsonResponseException response =
                                    (GoogleJsonResponseException) thrown.getCause();
                            assertThat(response.getStatusCode()).isEqualTo(401);
                        });

        install(obtained);
        FileSystem clientFs = plugin.create(bucketUri, client);
        FsPath path = new FsPath("gs://" + bucket + "/fluss-token-it/" + UUID.randomUUID());
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        write(clientFs, path, "hello");
        try (FSDataInputStream inputStream = clientFs.open(path)) {
            byte[] read = new byte[payload.length];
            assertThat(inputStream.read(read)).isEqualTo(payload.length);
            assertThat(read).isEqualTo(payload);
        }
        assertThat(clientFs.delete(path, false)).isTrue();
    }

    private static void installToken(String tokenValue) throws Exception {
        ObtainedSecurityToken token =
                new ObtainedSecurityToken(
                        "gs",
                        CredentialsJsonSerde.toJson(new Credentials("", "", tokenValue)),
                        System.currentTimeMillis() + 3_600_000L,
                        new HashMap<String, String>());
        new GSSecurityTokenReceiver().onNewTokensObtained(token);
    }

    private static void install(ObtainedSecurityToken token) throws Exception {
        new GSSecurityTokenReceiver().onNewTokensObtained(token);
    }

    private static void write(FileSystem fileSystem, FsPath path, String content) throws Exception {
        byte[] payload = content.getBytes(StandardCharsets.UTF_8);
        try (FSDataOutputStream outputStream = fileSystem.create(path, NO_OVERWRITE)) {
            outputStream.write(payload);
        }
    }
}
