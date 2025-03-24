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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/** A handle to a single file(a remote path after updated) of kv. */
public class KvFileHandle implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The path to the kv file. */
    private final String filePath;

    private final long size;

    public KvFileHandle(String fileHandle, long size) {
        this.filePath = fileHandle;
        this.size = size;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KvFileHandle that = (KvFileHandle) o;
        return size == that.size && Objects.equals(filePath, that.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, size);
    }

    @Override
    public String toString() {
        return "KvFileHandle{" + "filePath=" + filePath + ", size=" + size + '}';
    }

    /**
     * Discard by deleting the file that stores the kv. If the parent directory of the kv is empty
     * after deleting the kv file, it is also deleted.
     *
     * @throws Exception Thrown, if the file deletion (not the directory deletion) fails.
     */
    public void discard() throws Exception {
        FsPath fsPath = new FsPath(filePath);
        final FileSystem fs = fsPath.getFileSystem();

        IOException actualException = null;
        boolean success = true;
        try {
            success = fs.delete(fsPath, false);
        } catch (IOException e) {
            actualException = e;
        }

        if (!success || actualException != null) {
            if (fs.exists(fsPath)) {
                throw Optional.ofNullable(actualException)
                        .orElse(
                                new IOException(
                                        "Unknown error caused the file '"
                                                + fsPath
                                                + "' to not be deleted."));
            }
        }
    }
}
