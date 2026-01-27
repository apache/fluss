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
import org.apache.fluss.fs.azure.token.MockAuthServer;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.net.URI;

/** Tests that validate the behavior of the Azure File System Plugin. */
class AbfsFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String CONFIG_PREFIX = "fs.azure.account";
    private static final String CLIENT_ID = "testClientId";
    private static final String CLIENT_SECRET = "testClientSecret";

    private static final String AZURE_ACCOUNT_KEY = "ZmFrZS1rZXkK";
    private static final String ENDPOINT_KEY = "http://localhost:8080";
    public static final String ABFS_FS_PATH = "abfs://flus@test.dfs.core.windows.net/test";

    private static MockAuthServer mockAuthServer;

    @BeforeAll
    static void setup() {
        mockAuthServer = MockAuthServer.create();
        final Configuration configuration = new Configuration();
        configuration.setString(CONFIG_PREFIX + ".oauth2.client.id", CLIENT_ID);
        configuration.setString(CONFIG_PREFIX + ".oauth2.client.secret", CLIENT_SECRET);
        configuration.setString(CONFIG_PREFIX + ".oauth2.client.endpoint", ENDPOINT_KEY);
        configuration.setString(CONFIG_PREFIX + ".key", AZURE_ACCOUNT_KEY);
        FileSystem.initialize(configuration, null);
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

    @AfterAll
    static void tearDown() throws IOException {
        mockAuthServer.close();
    }
}
