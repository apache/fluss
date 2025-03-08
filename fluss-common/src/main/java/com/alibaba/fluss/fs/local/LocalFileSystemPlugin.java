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

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemPlugin;

import java.net.URI;

/** A plugin for the {@link LocalFileSystem}. */
@Internal
public class LocalFileSystemPlugin implements FileSystemPlugin {

    @Override
    public String getScheme() {
        return LocalFileSystem.getLocalFsURI().getScheme();
    }

    @Override
    public FileSystem create(URI fsUri, Configuration configuration) {
        return LocalFileSystem.getSharedInstance();
    }
}
