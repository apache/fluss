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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable standard metadata stored in a KV snapshot {@code _METADATA} file. */
@Internal
public final class KvSnapshotFileMetadata {

    private final TableBucket tableBucket;
    private final long snapshotId;
    private final String snapshotLocation;
    private final List<FileHandle> sharedFiles;
    private final List<FileHandle> privateFiles;
    private final long incrementalSize;
    private final long logOffset;
    private final @Nullable Long rowCount;
    private final @Nullable List<AutoIncrementRange> autoIncrementRanges;

    /** Creates immutable standard KV snapshot file metadata. */
    public KvSnapshotFileMetadata(
            TableBucket tableBucket,
            long snapshotId,
            String snapshotLocation,
            List<FileHandle> sharedFiles,
            List<FileHandle> privateFiles,
            long incrementalSize,
            long logOffset,
            @Nullable Long rowCount,
            @Nullable List<AutoIncrementRange> autoIncrementRanges) {
        this.tableBucket = checkNotNull(tableBucket, "Table bucket must not be null.");
        this.snapshotId = snapshotId;
        this.snapshotLocation =
                checkNotNull(snapshotLocation, "Snapshot location must not be null.");
        this.sharedFiles = immutableCopy(sharedFiles, "Shared files must not be null.");
        this.privateFiles = immutableCopy(privateFiles, "Private files must not be null.");
        this.incrementalSize = incrementalSize;
        this.logOffset = logOffset;
        this.rowCount = rowCount;
        this.autoIncrementRanges =
                autoIncrementRanges == null
                        ? null
                        : immutableCopy(
                                autoIncrementRanges,
                                "Auto-increment ranges must not contain null entries.");
    }

    /** Returns the table bucket described by this metadata. */
    public TableBucket getTableBucket() {
        return tableBucket;
    }

    /** Returns the snapshot ID. */
    public long getSnapshotId() {
        return snapshotId;
    }

    /** Returns the snapshot location. */
    public String getSnapshotLocation() {
        return snapshotLocation;
    }

    /** Returns the shared snapshot files. */
    public List<FileHandle> getSharedFiles() {
        return sharedFiles;
    }

    /** Returns the private snapshot files. */
    public List<FileHandle> getPrivateFiles() {
        return privateFiles;
    }

    /** Returns the incremental snapshot size. */
    public long getIncrementalSize() {
        return incrementalSize;
    }

    /** Returns the next log offset at snapshot time. */
    public long getLogOffset() {
        return logOffset;
    }

    /** Returns the row count when present. */
    @Nullable
    public Long getRowCount() {
        return rowCount;
    }

    /** Returns the auto-increment ranges when present. */
    @Nullable
    public List<AutoIncrementRange> getAutoIncrementRanges() {
        return autoIncrementRanges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KvSnapshotFileMetadata that = (KvSnapshotFileMetadata) o;
        return snapshotId == that.snapshotId
                && incrementalSize == that.incrementalSize
                && logOffset == that.logOffset
                && Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(snapshotLocation, that.snapshotLocation)
                && Objects.equals(sharedFiles, that.sharedFiles)
                && Objects.equals(privateFiles, that.privateFiles)
                && Objects.equals(rowCount, that.rowCount)
                && Objects.equals(autoIncrementRanges, that.autoIncrementRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableBucket,
                snapshotId,
                snapshotLocation,
                sharedFiles,
                privateFiles,
                incrementalSize,
                logOffset,
                rowCount,
                autoIncrementRanges);
    }

    private static <T> List<T> immutableCopy(List<T> values, String message) {
        checkNotNull(values, message);
        ArrayList<T> copy = new ArrayList<>(values.size());
        for (T value : values) {
            copy.add(checkNotNull(value, message));
        }
        return Collections.unmodifiableList(copy);
    }

    /** Immutable file reference stored in standard KV snapshot metadata. */
    @Internal
    public static final class FileHandle {

        private final String path;
        private final long size;
        private final String localPath;

        /** Creates an immutable file reference. */
        public FileHandle(String path, long size, String localPath) {
            this.path = checkNotNull(path, "File path must not be null.");
            this.size = size;
            this.localPath = checkNotNull(localPath, "File local path must not be null.");
        }

        /** Returns the remote file path. */
        public String getPath() {
            return path;
        }

        /** Returns the file size. */
        public long getSize() {
            return size;
        }

        /** Returns the local-path identity stored in the metadata. */
        public String getLocalPath() {
            return localPath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileHandle that = (FileHandle) o;
            return size == that.size
                    && Objects.equals(path, that.path)
                    && Objects.equals(localPath, that.localPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, size, localPath);
        }
    }

    /** Immutable auto-increment range stored in standard KV snapshot metadata. */
    @Internal
    public static final class AutoIncrementRange {

        private final int columnId;
        private final long start;
        private final long end;

        /** Creates an immutable auto-increment range. */
        public AutoIncrementRange(int columnId, long start, long end) {
            this.columnId = columnId;
            this.start = start;
            this.end = end;
        }

        /** Returns the auto-increment column ID. */
        public int getColumnId() {
            return columnId;
        }

        /** Returns the range start. */
        public long getStart() {
            return start;
        }

        /** Returns the range end. */
        public long getEnd() {
            return end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AutoIncrementRange that = (AutoIncrementRange) o;
            return columnId == that.columnId && start == that.start && end == that.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnId, start, end);
        }
    }
}
