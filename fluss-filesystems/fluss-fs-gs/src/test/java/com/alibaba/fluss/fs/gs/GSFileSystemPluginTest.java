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

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FSDataInputStream;
import com.alibaba.fluss.fs.FSDataOutputStream;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.testutils.common.CommonTestUtils;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests that validate the behavior of the Google Cloud Storage File System Plugin. */
class GSFileSystemPluginTest {

    private MockAuthServer mockGSServer;

    @BeforeEach
    void setUp() {
        mockGSServer = MockAuthServer.create();
    }

    @Test
    void testWithPluginManager() throws Exception {
        FileSystem fileSystem = createFileSystem();

        String basePath = "gs://test-bucket/fluss";
        assertThat(FileSystem.get(URI.create(basePath))).isInstanceOf(GSFileSystem.class);

        FsPath path = new FsPath(basePath, "test");
        final FSDataOutputStream outputStream =
                fileSystem.create(path, FileSystem.WriteMode.OVERWRITE);
        final byte[] testbytes = {1, 2, 3, 4, 5};
        outputStream.write(testbytes);
        outputStream.close();

        // check the path
        assertThat(fileSystem.exists(path)).isTrue();

        // try to read the file
        byte[] testbytesRead = new byte[5];
        FSDataInputStream inputStream = fileSystem.open(path);
        assertThat(5).isEqualTo(inputStream.read(testbytesRead));
        inputStream.close();
        assertThat(testbytesRead).isEqualTo(testbytes);

        assertThat(fileSystem.exists(path)).isTrue();

        // try to seek the file
        inputStream = fileSystem.open(path);
        inputStream.seek(4);
        testbytesRead = new byte[1];
        assertThat(1).isEqualTo(inputStream.read(testbytesRead));
        assertThat(testbytesRead).isEqualTo(new byte[] {testbytes[4]});

        // now delete the file
        assertThat(fileSystem.delete(path, true)).isTrue();
        // get the status of the file should throw exception
        assertThatThrownBy(() -> fileSystem.getFileStatus(path))
                .isInstanceOf(FileNotFoundException.class);
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
                            GoogleCloudStorageFileSystemOptions.DEFAULT
                                    .toBuilder()
                                    .setCloudStorageOptions(inMemoryGoogleCloudStorage.getOptions())
                                    .build());

            inMemoryGoogleCloudStorage.createBucket("test-bucket");
            Supplier<GoogleCloudStorageFileSystem> gsFs = () -> googleCloudStorageFileSystem;

            FieldUtils.writeField(fs, "gcsFsSupplier", gsFs, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        mockGSServer.close();
    }
}
