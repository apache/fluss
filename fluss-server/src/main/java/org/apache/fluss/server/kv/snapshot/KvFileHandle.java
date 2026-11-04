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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;

import javax.annotation.Nullable;

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

    private final @Nullable String sha256;

    public KvFileHandle(String fileHandle, long size) {
        this(fileHandle, size, null);
    }

    /** Creates a file handle with an optional lowercase hexadecimal SHA-256 digest. */
    public KvFileHandle(String fileHandle, long size, @Nullable String sha256) {
        this.filePath = fileHandle;
        this.size = size;
        this.sha256 = sha256;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getSize() {
        return size;
    }

    /** Returns the expected lowercase hexadecimal SHA-256 when present. */
    @Nullable
    public String getSha256() {
        return sha256;
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
        return size == that.size
                && Objects.equals(filePath, that.filePath)
                && Objects.equals(sha256, that.sha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, size, sha256);
    }

    @Override
    public String toString() {
        return "KvFileHandle{"
                + "filePath="
                + filePath
                + ", size="
                + size
                + ", sha256="
                + sha256
                + '}';
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
