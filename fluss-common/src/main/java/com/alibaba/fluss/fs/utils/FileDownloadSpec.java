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

package com.alibaba.fluss.fs.utils;

import com.alibaba.fluss.fs.FsPathAndFileName;

import java.nio.file.Path;
import java.util.List;

/**
 * A download spec to describe the file download request to download files {@link
 * #fsPathAndFileNames} to {@link #targetDirectory}.
 */
public class FileDownloadSpec {

    private final List<FsPathAndFileName> fsPathAndFileNames;

    private final Path targetDirectory;

    public FileDownloadSpec(List<FsPathAndFileName> fsPathAndFileNames, Path targetDirectory) {
        this.fsPathAndFileNames = fsPathAndFileNames;
        this.targetDirectory = targetDirectory;
    }

    public List<FsPathAndFileName> getFileHandles() {
        return fsPathAndFileNames;
    }

    public Path getTargetDirectory() {
        return targetDirectory;
    }
}
