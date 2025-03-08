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
import com.alibaba.fluss.fs.FileStatus;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;

import java.io.File;

/**
 * The class <code>LocalFileStatus</code> provides an implementation of the {@link FileStatus}
 * interface for the local file system.
 */
@Internal
public class LocalFileStatus implements FileStatus {

    /** The file this file status belongs to. */
    private final File file;

    /** The path of this file status belongs to. */
    private final FsPath path;

    /** Cached length field, to avoid repeated native/syscalls. */
    private final long len;

    /**
     * Creates a <code>LocalFileStatus</code> object from a given {@link File} object.
     *
     * @param f the {@link File} object this <code>LocalFileStatus</code> refers to
     * @param fs the file system the corresponding file has been read from
     */
    public LocalFileStatus(final File f, final FileSystem fs) {
        this.file = f;
        this.path = new FsPath(fs.getUri().getScheme() + ":" + f.toURI().getPath());
        this.len = f.length();
    }

    @Override
    public long getLen() {
        return this.len;
    }

    @Override
    public boolean isDir() {
        return this.file.isDirectory();
    }

    @Override
    public FsPath getPath() {
        return this.path;
    }

    public File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return "LocalFileStatus{" + "file=" + file + ", path=" + path + '}';
    }
}
