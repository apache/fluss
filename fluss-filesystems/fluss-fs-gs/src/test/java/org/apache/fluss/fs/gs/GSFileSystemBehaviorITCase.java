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
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.testutils.common.CommonTestUtils;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Tests that validate the behavior of the Google Cloud Storage File System Plugin. */
class GSFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static MockAuthServer mockGSServer;
    private static FileSystem fileSystem;

    @BeforeAll
    static void setup() throws IOException {
        mockGSServer = MockAuthServer.create();
        fileSystem = createFileSystem();
    }

    @Override
    protected FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath("gs://test-bucket/fluss");
    }

    private static FileSystem createFileSystem() throws IOException {
        Map<String, String> map = new HashMap<>();
        map.put("GCE_METADATA_HOST", "localhost:8080");
        CommonTestUtils.setEnv(map);

        String path =
                GSFileSystemPlugin.class
                        .getClassLoader()
                        .getResource("fake-service-account.json")
                        .getPath();

        GSFileSystemPlugin gsFileSystemPlugin = new GSFileSystemPlugin();
        Configuration configuration = new Configuration();
        configuration.setString("fs.gs.storage.root.url", "http://localhost:8080");
        configuration.setString("fs.gs.token.server.url", "http://localhost:8080/token");
        configuration.setString("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
        configuration.setString("fs.gs.auth.service.account.json.keyfile", path);
        configuration.setString("fs.gs.inputstream.support.gzip.encoding.enable", "false");

        FileSystem fileSystem =
                gsFileSystemPlugin.create(URI.create("gs://test-bucket/flusspath"), configuration);

        applyInMemoryStorage(fileSystem);

        return fileSystem;
    }

    private static void applyInMemoryStorage(FileSystem fileSystem) throws IOException {
        try {
            Object fs = FieldUtils.readField(fileSystem, "fs", true);
            final InMemoryGoogleCloudStorage inMemoryGoogleCloudStorage =
                    new InMemoryGoogleCloudStorage();
            GoogleCloudStorageFileSystem googleCloudStorageFileSystem =
                    new GoogleCloudStorageFileSystem(
                            googleCloudStorageOptions -> inMemoryGoogleCloudStorage,
                            GoogleCloudStorageFileSystemOptions.DEFAULT.toBuilder()
                                    .setCloudStorageOptions(inMemoryGoogleCloudStorage.getOptions())
                                    .build());

            inMemoryGoogleCloudStorage.createBucket("test-bucket");
            Supplier<GoogleCloudStorageFileSystem> gsFs = () -> googleCloudStorageFileSystem;

            FieldUtils.writeField(fs, "gcsFsSupplier", gsFs, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    static void tearDown() throws IOException {
        mockGSServer.close();
    }
}
