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

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * A wrapper of a file which contains a {@link FsPath} to the file and the corresponding file name.
 *
 * <p>It's mainly to represent the file uploaded to remote file system, {@link #path} is the remote
 * path to the file, and {@link #fileName} is the file name.
 *
 * @since 0.1
 */
@PublicEvolving
public class FsPathAndFileName {
    private final FsPath path;
    private final String fileName;

    public FsPathAndFileName(FsPath path, String fileName) {
        this.path = path;
        this.fileName = fileName;
    }

    public FsPath getPath() {
        return path;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FsPathAndFileName that = (FsPathAndFileName) o;
        return Objects.equals(path, that.path) && Objects.equals(fileName, that.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, fileName);
    }

    @Override
    public String toString() {
        return "FsPathAndFileName{"
                + "path='"
                + path
                + '\''
                + ", fileName='"
                + fileName
                + '\''
                + '}';
    }
}
