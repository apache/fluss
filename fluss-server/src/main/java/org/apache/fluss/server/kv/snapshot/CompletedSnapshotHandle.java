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

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A handle to a completed snapshot which contains the metadata file path to the completed snapshot.
 * It is as a wrapper around a {@link CompletedSnapshot} to make the referenced completed snapshot
 * retrievable through a simple get call.
 */
public class CompletedSnapshotHandle {

    private final long snapshotId;
    private final FsPath metadataFilePath;
    private final long logOffset;

    public CompletedSnapshotHandle(long snapshotId, FsPath metadataFilePath, long logOffset) {
        checkNotNull(metadataFilePath);
        this.snapshotId = snapshotId;
        this.metadataFilePath = metadataFilePath;
        this.logOffset = logOffset;
    }

    public CompletedSnapshot retrieveCompleteSnapshot() throws IOException {
        FSDataInputStream in = openInputStream();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        IOUtils.copyBytes(in, outputStream);
        return CompletedSnapshotJsonSerde.fromJson(outputStream.toByteArray());
    }

    public FSDataInputStream openInputStream() throws IOException {
        return metadataFilePath.getFileSystem().open(metadataFilePath);
    }

    /**
     * Gets the file system that stores the file state.
     *
     * @return The file system that stores the file state.
     * @throws IOException Thrown if the file system cannot be accessed.
     */
    private FileSystem getFileSystem() throws IOException {
        return FileSystem.get(metadataFilePath.toUri());
    }

    public FsPath getMetadataFilePath() {
        return metadataFilePath;
    }

    public long getLogOffset() {
        return logOffset;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public void discard() throws Exception {
        final FileSystem fs = getFileSystem();

        IOException actualException = null;
        boolean success = true;
        try {
            success = fs.delete(metadataFilePath, false);
        } catch (IOException e) {
            actualException = e;
        }

        if (!success || actualException != null) {
            if (fs.exists(metadataFilePath)) {
                throw Optional.ofNullable(actualException)
                        .orElse(
                                new IOException(
                                        "Unknown error caused the file '"
                                                + metadataFilePath
                                                + "' to not be deleted."));
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompletedSnapshotHandle that = (CompletedSnapshotHandle) o;
        return Objects.equals(metadataFilePath, that.metadataFilePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadataFilePath);
    }

    @Override
    public String toString() {
        return "CompletedSnapshotHandle{" + "filePath=" + metadataFilePath + '}';
    }
}
