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

import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FsPath;

import java.util.Date;

/** Minimal {@link FileStatus} for objects and (virtual) directories in BOS. */
class BOSFileStatus implements FileStatus {
    private final FsPath path;
    private final long len;
    private final boolean dir;
    private final long mtime;

    private BOSFileStatus(FsPath path, long len, boolean dir, long mtime) {
        this.path = path;
        this.len = len;
        this.dir = dir;
        this.mtime = mtime;
    }

    static BOSFileStatus file(FsPath path, long len, Date lastModified) {
        return new BOSFileStatus(
                path, len, false, lastModified == null ? 0L : lastModified.getTime());
    }

    static BOSFileStatus dir(FsPath path) {
        return new BOSFileStatus(path, 0L, true, 0L);
    }

    @Override
    public long getLen() {
        return len;
    }

    @Override
    public boolean isDir() {
        return dir;
    }

    @Override
    public FsPath getPath() {
        return path;
    }

    @Override
    public long getModificationTime() {
        return mtime;
    }

    @Override
    public String toString() {
        return (dir ? "dir " : "file ") + path + (dir ? "" : " (" + len + " bytes)");
    }
}
