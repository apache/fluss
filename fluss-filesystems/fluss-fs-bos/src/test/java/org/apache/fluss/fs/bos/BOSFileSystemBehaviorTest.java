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

package org.apache.fluss.fs.bos;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FsPath;

import com.baidubce.services.bos.FakeBosClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.Collections;
import java.util.UUID;

/**
 * Runs the common {@link FileSystemBehaviorTestSuite} against {@link BOSFileSystem} backed by an
 * in-memory BOS client, so the behavior contract is checked without a real bucket.
 */
class BOSFileSystemBehaviorTest extends FileSystemBehaviorTestSuite {

    private static final String BUCKET = "test-bucket";

    private final BOSFileSystem bosFs =
            new BOSFileSystem(BUCKET, Collections.emptyMap(), false, new FakeBosClient());

    @BeforeAll
    static void setup() {
        // lets FsPath#getFileSystem() resolve the bos scheme; nothing connects at creation time
        Configuration conf = new Configuration();
        conf.setString("bos.endpoint", "http://127.0.0.1:1");
        conf.setString("bos.access.key", "ak");
        conf.setString("bos.secret.key", "sk");
        FileSystem.initialize(conf, null);
    }

    @AfterAll
    static void clearFsConfig() {
        FileSystem.initialize(new Configuration(), null);
    }

    @Override
    protected FileSystem getFileSystem() {
        return bosFs;
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath("bos://" + BUCKET + "/tests-" + UUID.randomUUID());
    }
}
