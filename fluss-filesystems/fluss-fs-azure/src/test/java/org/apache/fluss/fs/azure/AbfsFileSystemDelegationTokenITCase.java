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

package org.apache.fluss.fs.azure;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.azure.token.AbfsDelegationTokenReceiver;
import org.apache.fluss.fs.azure.token.AzureDelegationTokenReceiver;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests that validate the Azure File System Plugin initializes correctly via the delegation token
 * path — i.e., when no {@code fs.azure.account.key} is configured and the client receives an OAuth
 * token from the server instead. This is the code path exercised by Fluss clients reading KV
 * snapshots from remote Azure storage.
 */
class AbfsFileSystemDelegationTokenITCase extends FileSystemBehaviorTestSuite {

    private static final String ABFS_FS_PATH = "abfs://fluss@test.dfs.core.windows.net/test";

    @BeforeAll
    static void setup() throws Exception {
        // Simulate the client receiving a delegation token from the server.
        // No fs.azure.account.key is set — this is the client-side path.
        Credentials credentials = new Credentials(null, null, "fake-oauth-access-token");
        Map<String, String> additionInfos = new HashMap<>();
        additionInfos.put(
                AzureFileSystemOptions.ENDPOINT_KEY.key(),
                "https://login.microsoftonline.com/fake-tenant/oauth2/token");
        ObtainedSecurityToken token =
                new ObtainedSecurityToken(
                        "abfs",
                        CredentialsJsonSerde.toJson(credentials),
                        System.currentTimeMillis() + 3_600_000L,
                        additionInfos);
        new AbfsDelegationTokenReceiver().onNewTokensObtained(token);

        // Initialize without account key.
        FileSystem.initialize(new Configuration(), null);
    }

    @AfterAll
    static void tearDown() throws Exception {
        // Reset static token state so other tests in the same JVM are not affected.
        FieldUtils.writeStaticField(
                AzureDelegationTokenReceiver.class, "additionInfos", null, true);
        FieldUtils.writeStaticField(AzureDelegationTokenReceiver.class, "credentials", null, true);
        FieldUtils.writeStaticField(AzureDelegationTokenReceiver.class, "validUntil", null, true);
    }

    @Override
    protected FileSystem getFileSystem() throws IOException {
        return getBasePath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() throws IOException {
        FsPath fsPath = new FsPath(ABFS_FS_PATH);
        applyMockStorage(fsPath.getFileSystem());
        return fsPath;
    }

    private static void applyMockStorage(FileSystem fileSystem) throws IOException {
        try {
            MemoryFileSystem memoryFileSystem = new MemoryFileSystem(URI.create(ABFS_FS_PATH));
            FieldUtils.writeField(fileSystem, "fs", memoryFileSystem, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
