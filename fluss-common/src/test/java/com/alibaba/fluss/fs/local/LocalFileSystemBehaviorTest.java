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

package com.alibaba.fluss.fs.local;

import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemBehaviorTestSuite;
import com.alibaba.fluss.fs.FsPath;

import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

/** Behavior tests for Fluss's {@link LocalFileSystem}. */
class LocalFileSystemBehaviorTest extends FileSystemBehaviorTestSuite {

    @TempDir private Path tmp;

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return LocalFileSystem.getSharedInstance();
    }

    @Override
    protected FsPath getBasePath() throws Exception {
        return new FsPath(tmp.toUri());
    }
}
