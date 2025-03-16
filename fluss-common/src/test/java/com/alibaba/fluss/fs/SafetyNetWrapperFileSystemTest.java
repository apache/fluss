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

package com.alibaba.fluss.fs;

import com.alibaba.fluss.fs.local.LocalFileSystem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SafetyNetWrapperFileSystem}. */
public class SafetyNetWrapperFileSystemTest extends FileSystemBehaviorTestSuite {

    @TempDir private Path tmp;

    @BeforeEach
    void prepare() throws Exception {
        FileSystemSafetyNet.initializeSafetyNetForThread();
        super.prepare();
    }

    @AfterEach
    void cleanup() throws Exception {
        FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        super.cleanup();
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return FileSystem.get(LocalFileSystem.getLocalFsURI());
    }

    @Test
    void testWrappedDelegate() {
        assertThat(fs).isInstanceOf(SafetyNetWrapperFileSystem.class);
        assertThat(((SafetyNetWrapperFileSystem) fs).getWrappedDelegate())
                .isInstanceOf(LocalFileSystem.class);
    }

    @Override
    protected FsPath getBasePath() throws Exception {
        return new FsPath(tmp.toUri());
    }
}
